# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import os
import logging
import logging.config
from conf.logger import client_log_config

import multiprocessing
import time
import setproctitle

#from mobyle.common.config import Config
#config = Config( os.path.join( os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection
from mobyle.common.job import Job, ClJob, Status


         
class DBManager(multiprocessing.Process):
    """synchronize the data base with the job in the system
     * get new entry in database and put the corresponding job in the JobsTable
     * update the status of running job from the system in the database
     * remove the completed jobs from the jobsTable
    
    """
    
    def __init__(self, jobs_table, master_q):
        """
        :param jobs_table: the container shared by all execution_engine members and containing all :mod:`mobyle.common.job` object alive in the system
        :type jobs_table: :class:`lib.execution_engine.jobstable.JobsTable` instance 
        :param master_q: a communication queue to listen comunication emit by the :class:`bin.mobexecd.Master` instance
        :type master_q: :class:`multiprocessing.Queue` instance
        
        """
        super(DBManager, self).__init__()
        self.master_q = master_q
        self.jobs_table = jobs_table
        self._log = None
        
    def run(self):
        self._name = "DBManager-%d" % self.pid
        setproctitle.setproctitle('mob2_DBManager')
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger( __name__ ) 
        while True :
            try:
                from_master = self.master_q.get(False) if not self.master_q.empty() else None
            except IOError:
                #[Errno 32] Broken pipethe Master does not respond anymore
                #then the jobsTable is down too
                break
            if from_master == 'STOP':
                self.stop()
                break
            elif from_master == 'RELOAD':
                self.reload_conf()
            try:
                jobs_to_update = self.jobs_table.jobs()
            except IOError:
                #[Errno 32] Broken pipe the Master does not respond anymore
                #then the jobsTable is down too
                break
            self.update_jobs(jobs_to_update)
            active_jobs = self.get_active_jobs()
            for job in active_jobs:
                self.jobs_table.put(job)
            time.sleep(2)
     
    def stop(self):
        """
        update the jobs in the DB with jobs informations from the JobsTable before exiting
        """
        jobs_to_update = self.jobs_table.jobs()
        self.update_jobs(jobs_to_update)       
            
    def update_jobs(self, jobs_to_update):
        """
        synchronize the db with the jobs from the jobs_table
        and remove completed jobs from the jobs table
        
        :param jobs_to_update: the jobs to update
        :type jobs_to_update: list of :mod:`mobyle.common.job` instance. 
        """
        for job in jobs_to_update:
            #mise a jour de tous les jobs
            job.save() 
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

    def get_active_jobs(self):
        """
        :returns: the all the jobs that should handle by the exec_engine
        :rtype: list of :mod:`mobyle.common.job`
        """
        #entries is a cursor (a kind of generator, NOT a list
        #I assume that we will not have too many jobs at one time
        #check if it's always the case even after the exec_egine is stopped for a while and restart
        #while the portal continue to accept new jobs 
        entries = list(connection.Job.find({'status': { '$in' : Status.active_states() }}))
        self._log.debug("%s new entries = %s"%(self.name, [en.id for en in entries]))           
        
        #check if a job is already in jobs_table 
        active_jobs_id = [j.id for j in self.jobs_table.jobs()]
        self._log.debug( "%s active_jobs_id = %s (%d)"%(self._name, active_jobs_id, len(active_jobs_id)))
        new_jobs = [j for j in entries if j.id not in active_jobs_id]
        self._log.debug( "%s new_jobs = %s"%(self._name, [j.id for j in new_jobs]))
        return new_jobs

    
    def reload_conf(self):
        #relire la conf
        self.self._log.debug("%s reload() relit sa conf" % self._name)
        pass  
        