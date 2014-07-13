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
import setproctitle

        
class NotificationActor(multiprocessing.Process):
    """
    notify that the job corresponding to job_id is completed
    """
    
    def __init__(self, table, job_id ):
        """
        :param jobs_table: the container shared by all containing all :class:`lib.execution_engine.jobref.JobRef` alive in the system
        :type jobs_table: :class:`lib.execution_engine.jobstable.JobsTable` instance 
        :param job_id: the id of the job to treat
        :type job_id: string
        """
        super(NotificationActor, self).__init__()
        self._log = None
        self.table = table  
        self.job_id = job_id
           
    def run(self):
        self._name = "NotificationActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_notification')
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger(__name__) 
        
        job = self.table.get(self.job_id )
        if job.status.is_ended() and job.must_be_notified():
            job.has_been_notified = True
            self._log.debug( "{0} notified job {1} and put it in table".format(self._name, job.id))
            self.table.put( job )
        else:
            self._log.debug( "{0} job {1} must not be notified".format(self._name, job.id))
        self._log.debug( "{0} exiting".format(self._name))
        
        
