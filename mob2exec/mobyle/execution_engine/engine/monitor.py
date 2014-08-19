# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import logging
import logging.config
import multiprocessing
import time
import setproctitle

from mobyle.common.connection import connection
from mobyle.common.job import Job, ProgramJob, Status
from mobyle.common.config import Config

from .build_actor import BuildActor
from .submit_actor import SubmitActor
from .status_actor import StatusActor
from .notification_actor import NotificationActor


class JobsTable(object):
    """
    Manage the list of the active job ids 
    """
    
    def __init__(self):
        self.jobs_table = []
        
    def __iter__(self):
        return iter(self.jobs_table)
    
    def put(self, job_id):
        """add a Job id instance in table
        
        :param job_id: the job identifier to add in the table
        :type job: string
        """
        self.jobs_table.append(job_id)
         
    def jobs(self):
        """
        :return: the list of job identifiers contained in the table.
        :rtype: list string
        
        """
        jobs = self.jobs_table[:]
        jobs.sort()
        return jobs
    
    def pop(self, job_id):
        """
        :param job_id: the id of a Job instance
        :type job_id: string
        :return: and remove the Job identifier from the table.
        :rtype: :class:`mobyle.common.job` instance
        
        """
        idx = self.jobs_table.index(job_id)
        jobid = self.jobs_table[idx]
        self.jobs_table.remove(jobid)
        return jobid
        
        
        
class JtMonitor(multiprocessing.Process):
    """
    The JtMonitor monitor the jobs table and for each job start a new actor 
    according the job status.  
    """
    
    def __init__(self, master_q, cfg):
        """
        :param master_q: a communication queue to listen comunication emit by the :class:`bin.mob2execd.Master` instance
        :type master_q: `multiprocessing.Queue` instance
        """
        super(JtMonitor , self).__init__()
        self.master_q = master_q
        self.jobs_table = JobsTable()
        self._child_process = []
        self._log_config = self.get_log_config(cfg)
        self._log = None
        
        
    def run(self):
        self._name = "Monitor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_monitor')
        logging.config.dictConfig(self._log_config)
        self._log = logging.getLogger(__name__) 
        
        while True :
            try:
                from_master = self.master_q.get(False) if not self.master_q.empty() else None
            except IOError:
                #[Errno 32] Broken pipe the Master does not respond anymore
                self.stop()
                break
            if from_master == 'STOP':
                self.stop()
                break
            elif from_master == 'RELOAD':
                self.reload_conf()
            try:
                all_job_ids = self.jobs_table.jobs() 
            except IOError:
                #[Errno 32] Broken pipe the Master does not respond anymore
                self.stop()
                break
            for job_id in all_job_ids:
                try:
                    job = connection.Job.fetch_one({'_id' : job_id})
                except Exception, err:
                    self._log.error(str(err), exec_info = True)
                    # TO BE IMPROVE
                    raise err
                if job is None:
                    self._log.Error('try to fetch job id = {0} which was not found in DB'.format(job_id))
                if job.status.is_buildable():
                    actor = BuildActor(job.id, self._log_config)
                    self._log.debug("{0} start a new BuildActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_submittable() :
                    actor = SubmitActor(job.id, self._log_config)
                    self._log.debug( "{0} start a new SubmitActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_queryable():
                    actor = StatusActor(job.id, self._log_config)
                    self._log.debug("{0} start a new StatusActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_ended() and job.must_be_notified() and not job.has_been_notified:
                    actor = NotificationActor(job.id, self._log_config)
                    self._log.debug("{0} start a new NotificationActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
            time.sleep(2)
            #remove zombies
            for p in self._child_process:
                if not p.is_alive():
                    self._child_process.remove(p)
            #uptdate table
            self.remove_ended_job(self.jobs_table.jobs())
            for job_id in self.get_active_jobs():
                self.jobs_table.put(job_id)
                
    
    def get_active_jobs(self):
        """
        :returns: the all the job_ids that should handle by the exec_engine
        :rtype: list of string
        """
        #entries is a cursor (a kind of generator, NOT a list
        #I assume that we will not have too many jobs at one time
        #check if it's always the case even after the exec_egine is stopped for a while and restart
        #while the portal continue to accept new jobs 
       
        #entries = list(connection.ProgramJob.find({'status': { '$in' : Status.active_states() }}))
        entries = list(connection.Job.find({'status': { '$in' : Status.active_states() }}))
        self._log.debug("{0} new entries = {1}".format(self.name, [en.id for en in entries]))           
        
        #check if a job is already in jobs_table 
        active_jobs_id = self.jobs_table.jobs()
        self._log.debug("{0} active_jobs_id = {1} ({2:d})".format(self._name, active_jobs_id, len(active_jobs_id)))
        new_jobs = [j.id for j in entries if j.id not in active_jobs_id]
        self._log.debug("{0} new_jobs = {1}".format(self._name, new_jobs))
        return new_jobs
    
    
    def remove_ended_job(self, job_ids):
        """
        remove completed jobs from the jobs table
        
        :param jobs_to_update: the jobs to update
        :type jobs_to_update: list of :mod:`mobyle.common.job` instance. 
        """
        for job_id in job_ids:
            job = connection.Job.fetch_one({'_id' : job_id})
            if job.status.is_ended() :
                if job.must_be_notified():
                    if job.has_been_notified:
                        self.jobs_table.pop(job.id)
                    else:
                        pass
                else:
                    self.jobs_table.pop(job.id)
            else:
                pass
                   
    def get_log_config(self, cfg):
        
        client_log_config = { 'version' : 1 ,
                'disable_existing_loggers': True,
               'handlers': {
                            'socket':{
                                       'class' : 'logging.handlers.SocketHandler',
                                       'host' : 'localhost',
                                       'port' : 'ext://logging.handlers.DEFAULT_TCP_LOGGING_PORT',
                                       'level' : 'DEBUG',
                                       },
                           
                            'email': {
                                                 'class': 'logging.handlers.SMTPHandler' ,
                                                 'mailhost': cfg.get('mob2exec', 'log_email_mta'),
                                                 'fromaddr': cfg.get('mob2exec','log_email_from').split(),
                                                 'toaddrs' : cfg.get('mob2exec','log_email_to').split(),
                                                 'subject' : cfg.get('mob2exec','log_email_subject'), 
                                                 'level'   : cfg.get('mob2exec','log_email_level'),
                                                 },
                            }, 
               
               'loggers': { 'mobyle' : {
                                  'handlers': [ 'socket', 'email'] ,
                                  'level': 'NOTSET'
                                },
                           }
              }
        return client_log_config
                    
    def stop(self):
        """
        wait for children completion before exiting
        """
        self._log.debug("recieved STOP")
        for p in self._child_process:
            p.join()
            
    def reload_conf(self):
        """
        reload the configuration from mongo
        """
        #relire la conf
        self._log.debug("{0} reload() relit sa conf".format(self._name))
        

      
