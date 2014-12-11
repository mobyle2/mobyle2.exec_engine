# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

import os
import imp

from mobyle.common.error import InternalError
from mobyle.common.job import Status


from execution_system import ExecutionSystem



class DRMAA(ExecutionSystem):
    
    def __init__(self, name, drmaa_library_path = None, native_specifications = "", contact_string= ""):
        super(DRMAA, self).__init__(name)
        self.drmaa_library_path = drmaa_library_path
        os.environ[ 'DRMAA_LIBRARY_PATH' ] = self.drmaa_library_path
        fp , pathname , description = imp.find_module("drmaa")
        self.drmaa = imp.load_module("drmaa", fp, pathname, description)
        self.native_specifications = native_specifications
        self.contact_tring = contact_string
        
        
    def _drmaaStatus2mobyleStatus(self, drmaaStatus):
        """
        convert a drmaa status in to a state of :class:`mobyle.common.job.Status`
        
        :return: a state of mobyle Status
        :rtype: string  
        """
        if drmaaStatus == self.drmaa.JobState.RUNNING:
            return Status.RUNNING
        elif drmaaStatus == self.drmaa.JobState.UNDETERMINED:
            return Status.UNKNOWN
        elif drmaaStatus == self.drmaa.JobState.QUEUED_ACTIVE:
            return Status.PENDING 
        elif drmaaStatus == self.drmaa.JobState.DONE:
            return Status.FINISHED 
        elif drmaaStatus == self.drmaa.JobState.FAILED:
            return Status.ERROR
        elif drmaaStatus == self.drmaa.JobState.SYSTEM_ON_HOLD:
            return Status.HOLD
        elif drmaaStatus == self.drmaa.JobState.USER_ON_HOLD:
            return Status.HOLD
        elif drmaaStatus == self.drmaa.JobState.USER_SYSTEM_ON_HOLD:
            return Status.HOLD
        elif drmaaStatus == self.drmaa.JobState.SYSTEM_SUSPENDED:
            return Status.HOLD
        elif drmaaStatus == self.drmaa.JobState.USER_SUSPENDED:
            return Status.HOLD
        elif drmaaStatus == self.drmaa.JobState.USER_SYSTEM_SUSPENDED:
            return Status.HOLD
        else:
            return Status.UNKNOWN 
    
    def run(self, job):
        """
        submit a job asynchronously on DRM via the drmaa library.
        
        :param job: the job to run.
        :type job: :class:`mobyle.common.job.Job` object.
        :returns: the job number of the job_script
        :rtype: int
        """
        job_dir = os.path.normpath(job.dir)
        if os.getcwd() != job_dir:
            msg = "job {id} is not in right dir: {cwd} instead of {job_dir}".format(id = job.id, cwd = os.getcwd(), job_dir = job_dir)
            self._log.critical(msg)
            raise InternalError(message = msg)
        service_name = job.service.name
        try:
            fout = open(os.path.join(job_dir, service_name + '.out'), 'w')
            ferr = open(os.path.join(job_dir, service_name  + '.err'), 'w')
        except OSError as err:
            msg = 'cannot open job files in {job_dir} : {err}'.format(job_dir = job_dir, err = err)
            self._log.critical(msg)
            self.job.status = Status.ERROR
            raise InternalError(msg)
        else:
            fout.close()
            ferr.close()
            
        try:
            drmaaSession = self.drmaa.Session(contactString = self.contactString)
            try:
                drmaaSession.initialize()
            except self.drmaa.errors.AlreadyActiveSessionException:
                pass
            except Exception as err:
                msg = "error during drmaa intitialization for job : {job_dir} : {err}".format(job_dir = job_dir, err = err)
                self._log.critical(msg, exc_info= True)
                raise InternalError(msg)
            job_template = drmaaSession.createJobTemplate()
            job_template.workingDirectory = job_dir
            job_template.jobName = job.id
            job_template.outputPath = ":" + os.path.join(job_dir, fout.name )
            job_template.errorPath  = ":" + os.path.join(job_dir, ferr.name )
            job_template.joinFiles = False
            job_template.jobEnvironment = job.cmd_env
            job_template.remoteCommand = "/bin/sh"
            job_template.args = [os.path.join(job_dir, ".job_script")]
            job_template.nativeSpecification = self.native_specifications
            job_template.blockEmail = True
            drm_jobid = drmaaSession.runJob(job_template)
        except Exception as err:
            msg= "Local execution failed: job dir = {job_dir} : {err}".format(job_dir = job_dir, err = err)
            self._log.critical(msg, exc_info = True)
            raise InternalError(message = msg)
        return drm_jobid
        
        
    def get_status(self, job):
        """
        query the drm to get the status of a job
        translate it in Mobyle Status **and update** the job
        
        :param job: the job to query the status
        :type job: :class:`mobyle.common.job.Job` object.
        :return: the status of the job.
        :rtype: :class:`mobyle.common.job.Status` object.
        """
        job_dir = job.dir
        try:
            s = self.drmaa.Session( contactString = self.contactString )
        except Exception as err:
            self._log.error( "{job_dir} get_status cannot open drmma session : {err} ".format(job_dir = job_dir, err = err))
            return Status(Status.UNKNOWN)
        try:
            s.initialize()
        except self.drmaa.errors.AlreadyActiveSessionException:
            pass
        except Exception as err:
            s.exit()
            self._log.critical( "error during drmaa intitialization for getStatus job {job_dir} : {err}".format(job_dir = job_dir, err = err), exc_info= True )
            return Status(Status.UNKNOWN)
        
        job_no_in_drm = job.execution_job_no
        #job_no_in_drm should work with a string 
        try:
            drmaaStatus = s.jobStatus(job_no_in_drm)
        except Exception as err:
            s.exit()
            self._log.error("error duriing geting status for job {job_dir} : {err}".format(job_dir = job_dir, err = err))
            return Status(Status.UNKNOWN) 
        s.exit()
        return Status(self._drmaaStatus2mobyleStatus(drmaaStatus)) 
    
    
    def kill(self, job):
        """
        ask to the execution to terminate a job, and updated it.
        
        :param job: the job to kill.
        :type job: :class:`mobyle.common.job.Job` object.
        """
        job_no_in_drm = job.execution_job_no
        job_dir = job.dir
        try:
            s = self.drmaa.Session(contactString = self.contactString)
        except Exception as err:
            self._log.critical( "cannot kill job {job_dir}: cannot open drmma session : {err}".format(job_dir = job_dir, err = err), exc_info = True)
            return None
        try:
            s.initialize()
        except self.drmaa.errors.AlreadyActiveSessionException:
            pass
        except Exception as err:
            s.exit()
            self._log.critical( "error during drmaa intitialization for kill job {job_dir} : {err}".format(job_dir = job_dir, err = err), exc_info= True )
            return Status(Status.UNKNOWN)
        try:
            s.control(job_no_in_drm, self.drmaa.JobControlAction.TERMINATE )
        except Exception as err:
            msg = "cannot kill job {job_dir}: {err}".format(job_dir = job_dir, err = err)
            self._log.error(msg)
            raise InternalError(msg)
        finally:    
            s.exit()
        return None
