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
import os

from mobyle.common.job import Status       
from mobyle.common.mobyleError import MobyleError

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
        self._name = "BuildActor-{0:d}".format(self.pid)
        setproctitle.setproctitle('mob2_build')
        logging.config.dictConfig(client_log_config)
        self._log = logging.getLogger( __name__ ) 
        job = self.table.pop(self.job_id )
        
        self.make_job_environement(job)
        os.chdir(job.dir)
        
        #import data needed for the job
        
        #build the cmdline??? seulement pour ClJob ???
        #ou action generique de job et joue sur le polymorphism?
        
        #perform data conversion
        #how to decide which data must be convert?
        
        job.status.state = Status.TO_BE_SUBMITTED  
        self._log.info( "{0} put job {1} with status {2} in table".format(self._name, job.id, job.status))
        #the acces log must record 
        # the submited jobs to mobyle 
        #  or
        # the submitted job to execution?
        #
        #acc_log = logging.getLogger( 'access')
        #acc_log.info( "test access log %s" % self._name)
        self.table.put( job )
    
    
    def make_job_environement(self, job):
        """
        create the environment to run a job
          - working directory creation
          - fixing permission
        """
        project = job.get_project()
        job_dir = os.path.abspath(os.path.join(project.dir, 'jobs', job.id))
        if os.path.exists(job_dir):
                msg = 'cannot make job directory: {0} already exists'.format(job_dir)
                self._log.error(msg)
                raise MobyleError(msg)
        try:
            os.makedirs(job_dir, 0755) #create parent directory
        except Exception , err:
            self._log.critical( "unable to create job directory {0}: {1} ".format(job_dir, err), exc_info = True)
            raise MobyleError , "Internal server Error"
        os.umask(0022)
        job.dir = job_dir