# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2013
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import logging.config
import setproctitle
import os

from mobyle.common.job import Status       
from mobyle.common.mobyleError import MobyleError
from .actor import Actor

class BuildActor(Actor):
    """
    submit job to the execution system.
    """
    

    def __init__(self, job_id, log_conf):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(BuildActor, self).__init__(job_id, log_conf)
           
    def run(self):
        self._name = "BuildActor-{0:d} job {1}".format(self.pid, self.job_id)
        setproctitle.setproctitle('mob2_build')
        
        logging.config.dictConfig(self._log_conf)
        self._log = logging.getLogger(__name__) 
        
        #change the status to aware the job that this job is currently building  
        job = self.get_job()
        job.status.state = Status.BUILDING
        job.save()
        
        self.make_job_environement(job)
        os.chdir(job.dir)
        
        #import data needed for the job
        
        #build the cmdline??? seulement pour ClJob ???
        #ou action generique de job et joue sur le polymorphism?
        
        #perform data conversion
        #how to decide which data must be convert?
        
        # the acces log must record 
        # the submited jobs to mobyle 
        #  or
        # the submitted job to execution?
        #
        #acc_log = logging.getLogger( 'access')
        #acc_log.info( "test access log {0}".format(self._name))
        
        #the monitor is now aware of the new status
        job.status.state = Status.TO_BE_SUBMITTED
        job.save()
        self._log.info( "{0} put job {1} with status {2} in table".format(self._name, job.id, job.status))
    
    
    def make_job_environement(self, job):
        """
        create the environment to run a job
          - working directory creation
          - fixing permission
        """
        project = job.get_project()
        try:
            job_dir = os.path.abspath(os.path.join(project.dir, 'jobs', str(job.id)))
        except Exception, err:
            msg = "cannot build  the job dir the database may be corrupted project dir: {},  job id: {}".format(project.dir, job.id)
            self._log.critical(msg)
            raise MobyleError(msg)
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
        