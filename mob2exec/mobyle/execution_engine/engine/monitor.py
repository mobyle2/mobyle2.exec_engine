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
from conf.logger import client_log_config

import multiprocessing
import time
import setproctitle

from .build_actor import BuildActor
from .submit_actor import SubmitActor
from .status_actor import StatusActor
from .notification_actor import NotificationActor


        
class JtMonitor(multiprocessing.Process):
    """
    The JtMonitor monitor the jobs table and for each job start a new actor 
    according the job status.  
    """
    
    def __init__(self, jobs_table, master_q):
        """
        :param jobs_table: the container shared by all containing all JobRef alive in the system
        :type jobs_table: :class:`execution_engine.jobstable.JobsTable` instance 
        :param master_q: a communication queue to listen comunication emit by the :class:`bin.mob2execd.Master` instance
        :type master_q: `multiprocessing.Queue` instance
        """
        super( JtMonitor , self).__init__()
        self.master_q = master_q
        self.table = jobs_table
        self._child_process = []
        self._log = None
        self.dispatcher = dispatcher
        
        
    def run(self):
        self._name = "Monitor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_monitor')
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger( __name__ ) 
        
        from ..job_routing.route import dispatcher
        
        while True :
            try:
                from_master = self.master_q.get( False ) if not self.master_q.empty() else None
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
                all_jobs = self.table.jobs() 
            except IOError:
                #[Errno 32] Broken pipe the Master does not respond anymore
                self.stop()
                break
            for job in all_jobs:
                if job.status.is_buildable():
                    actor = BuildActor(self.table, job.id)
                    self._log.debug("{0} start a new BuildActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_submittable() :
                    route = self.dispatcher.which_route(job) 
                    job.route = route
                    self.table.put(job)
                    actor = SubmitActor(self.table, job.id)
                    self._log.debug( "{0} start a new SubmitActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_queryable():
                    actor = StatusActor(self.table, job.id)
                    self._log.debug("{0} start a new StatusActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
                elif job.status.is_ended() and job.must_be_notified() and not job.has_been_notified:
                    actor = NotificationActor(self.table, job.id)
                    self._log.debug("{0} start a new NotificationActor = {1} job = {2}".format(self._name, actor.name, job.id))
                    actor.start()
                    self._child_process.append(actor)
            time.sleep(2)
            #remove zombies
            for p in self._child_process:
                if not p.is_alive():
                    self._child_process.remove(p)
                    
                    
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
        

      
