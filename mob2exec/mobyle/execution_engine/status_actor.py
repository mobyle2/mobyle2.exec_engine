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

from mobyle.common.job import Status

####### BOUCHON ##########
import random
from datetime import datetime
##########################
        
class StatusActor(multiprocessing.Process):
    """
    get the status from the execution system for a job
    """
    
    def __init__(self, table, job_id ):
        """
        :param jobs_table: the container shared by all containing all :class:`lib.execution_engine.jobref.JobRef` alive in the system
        :type jobs_table: :class:`lib.execution_engine.jobstable.JobsTable` instance 
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(StatusActor, self).__init__()
        self._log = None
        self.table = table  
        self.job_id = job_id
           
    def run(self):
        self._name = "StatusActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_status')
        
        job = self.table.get(self.job_id)
        old_status = job.status.state
        
        job.status.state = Status.UPDATING
        self.table.put(job)
        
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger( __name__ ) 
        self._log.debug( "{0} set job {1} status {2} to {3}".format(self._name, 
                                                                    self.job_id, 
                                                                    old_status, 
                                                                    job.status.state))
        ####################### BOUCHON ###############################
        if old_status == Status.SUBMITTED:
            job.status.state = (Status.RUNNING, Status.PENDING, Status.HOLD)[random.randint(0, 2)]
        elif old_status == Status.PENDING:
            job.status.state = (Status.PENDING, Status.RUNNING, Status.HOLD, Status.FINISHED, Status.ERROR, Status.KILLED)[random.randint(0, 4)]
        elif old_status == Status.HOLD:
            job.status.state = (Status.RUNNING,  Status.HOLD, Status.FINISHED, Status.ERROR, Status.KILLED)[random.randint(0, 3)]
        elif old_status == Status.RUNNING:
            job.status.state = (Status.RUNNING,  Status.HOLD, Status.FINISHED, Status.ERROR, Status.KILLED)[random.randint(0, 3)]
        if job.status.is_ended():
            job.end_time = datetime.now()
        ####################### FIN BOUCHON #########################################
        self._log.debug("{0} set job {1} status {2} to {3}".format(self._name, 
                                                                   self.job_id, 
                                                                   Status.UPDATING, 
                                                                   job.status.state))
        #only now the monitor is aware of the new status
        self.table.put(job)
        self._log.debug("{0} exiting".format(self._name))
        
        
