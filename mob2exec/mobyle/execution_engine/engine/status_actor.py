# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import logging.config
import setproctitle

from mobyle.common.job import Status
from mobyle.common.mobyleError import MobyleError
from .actor import Actor

####### BOUCHON ##########
import random
from datetime import datetime
##########################
        
class StatusActor(Actor):
    """
    get the status from the execution system for a job
    """
    
    def __init__(self, job_id, log_conf):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(StatusActor, self).__init__(job_id, log_conf)

           
    def run(self):
        self._name = "StatusActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_status')
        
        logging.config.dictConfig(self._log_conf)
        self._log = logging.getLogger( __name__ ) 
        
        job = self.get_job()
        old_status = job.status.state
        job.status.state = Status.UPDATING
        job.save()
        
        exec_system_id = str(job.execution.exec_system_id)
        exec_system = self.get_execution_system(exec_system_id)
        
        self._log.debug( "{0} job {1} was submit with exec sytem {2} and have n° {3}".format(self._name, 
                                                                                             self.job_id,
                                                                                             exec_system.name,
                                                                                             job.execution.job_no))
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
        job.save()
        self._log.debug("{0} exiting".format(self._name))
        
        
