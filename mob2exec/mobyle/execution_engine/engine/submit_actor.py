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
import os

from mobyle.common.error import InternalError
from mobyle.common.job import Status
from ..job_routing.route import get_dispatcher
from .actor import Actor


class SubmitActor(Actor):
    """
    submit job to the execution system.
    """
    

    def __init__(self, job_id, log_conf):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(SubmitActor, self).__init__(job_id, log_conf)
        
           
    def run(self):
        self._name = "SubmitActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_submit')
        
        logging.config.dictConfig(self._log_conf)
        self._log = logging.getLogger(__name__) 
        
        job = self.get_job()
        job.status.state = Status.SUBMITTING
        job.save()
        
        try:
            os.chdir(job.dir)
        except OSError, err:
            msg = "cannot change working dir for job dir : {0}".format(err)
            self._log.critical(msg)
            raise InternalError(message = msg)
        
        dispatcher = get_dispatcher()
        route = dispatcher.which_route(job)
        exec_system = route.exec_sys
        
        project = job.get_project()
        
        ############### DEBUG #######################
        self._log.info( u"{0} job {1} (project = {2} ) has route {3}".format(self._name, job.id, project['name'], route.name))
        self._log.info( u"{0} job {1} use exec system {2}".format(self._name, job.id, exec_system.name))
        ############### DEBUG #######################
        
        job.execution_system_id = exec_system.name
        # submit the job
        job_pid = exec_system.run(job)
        job.execution_job_no = str(job_pid)
        job.save()
        
        
        self._log.info( "{0} put job {1} with status {2} in table".format(self._name, job.id, job.status))
        
        job.status.state = Status.SUBMITTED
        job.save()
        
        
