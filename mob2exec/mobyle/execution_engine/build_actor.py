# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2013
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

class BuildActor(multiprocessing.Process):
    """
    submit job to the execution system.
    """
    

    def __init__(self, table, job_id ):
        """
        :param jobs_table: the container shared by all containing all :class:`lib.execution_engine.jobref.JobRef` alive in the system
        :type jobs_table: :class:`lib.execution_engine.jobstable.JobsTable` instance 
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(BuildActor, self).__init__()
        self._log = None
        self.table = table  
        self.job_id = job_id
           
    def run(self):
        self._name = "BuildActor-%d" % self.pid
        setproctitle.setproctitle('mob2_build')
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger( __name__ ) 
        job = self.table.pop(self.job_id )
        job.status.state = Status.TO_BE_SUBMITTED  
        self._log.info( "%s put job %s with status %s in table" % ( self._name , job.id , job.status ) )
        #the acces log must record 
        # the submited jobs to mobyle 
        #  or
        # the submitted job to execution?
        #
        #acc_log = logging.getLogger( 'access')
        #acc_log.info( "test access log %s" % self._name)
        self.table.put( job )
        
        
