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
import setproctitle

from mobyle.common.job import Status       
from .actor import Actor

class SubmitActor(Actor):
    """
    submit job to the execution system.
    """
    

    def __init__(self, job_id):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(SubmitActor, self).__init__(job_id)

           
    def run(self):
        self._name = "SubmitActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_submit')
        
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger(__name__) 
        
        from ..job_routing.route import dispatcher
        
        job = self.get_job()
        job.status.state = Status.SUBMITTING
        job.save()
        
        ###################### 
        
        # here the code to submit a job
        route = dispatcher.which_route(job)
        exec_system = route.exec_sys
        
        project = job.get_project()
        self._log.info( u"{0} job {1} (project = {2} ) has route {3}".format(self._name, job.id, project['name'], route.name))
        self._log.info( u"{0} job {1} use exec system {2}".format(self._name, job.id, exec_system.name))
        #####################  
        #
        job.execution.exec_system_id = exec_system.name
        # submit the job
        import random
        job.execution.job_no = str(random.randint(1,1000))
        job.save()
        
        self._log.info( "{0} put job {1} with status {2} in table".format(self._name, job.id, job.status))
        
        job.status.state = Status.SUBMITTED
        job.save()
        
        
