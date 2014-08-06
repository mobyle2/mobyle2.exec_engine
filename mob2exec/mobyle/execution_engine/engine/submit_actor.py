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

class SubmitActor(multiprocessing.Process):
    """
    submit job to the execution system.
    """
    

    def __init__(self, table, job_id):
        """
        :param table: the container shared by all containing all :class:`lib.execution_engine.job.Job` alive in the system
        :type table: :class:`lib.execution_engine.jobstable.JobsTable` instance 
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(SubmitActor, self).__init__()
        self._log = None
        self.table = table  
        self.job_id = job_id
           
    def run(self):
        self._name = "SubmitActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_submit')
        
        job = self.table.get(self.job_id)
        job.status.state = Status.SUBMITTING
        self.table.put(job)
        
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger(__name__) 
        ###################### 
        
        # here the code to submit a job
        project = job.get_project()
        self._log.debug( u"### DEBUG SubmitActor job.route = {0} ####".format(job.route))
        self._log.debug( u"### DEBUG SubmitActor project = {0} ####".format(project['name']))
        self._log.debug( u"### DEBUG SubmitActor user = {0} ####".format(project['owner']))
        self._log.info( u"{0} job {1} (project = {2} ) has route {3}".format(self._name, job.id, project['name'], job.route.name))
        ######################  
        #
        # recuperer la classe d'executon pour ce job
        #   
        # faire run
        self._log.info( "{0} put job {1} with status {2} in table".format(self._name, job.id, job.status))
        acc_log = logging.getLogger('access')
        acc_log.info("test access log {0}".format(self._name))
        
        job.status.state = Status.SUBMITTED
        self.table.put(job)
        
        
