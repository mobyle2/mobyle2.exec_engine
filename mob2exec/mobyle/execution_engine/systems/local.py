# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

import os
from subprocess import Popen
from signal import SIG_DFL, SIGTERM, SIGKILL
from time import sleep

from mobyle.common.error import InternalError
from mobyle.common.job import Status
from mobyle.common.utils import which
from execution_system import ExecutionSystem, register

@register
class Local(ExecutionSystem):
    
    def __init__(self, name, native_specifications = ""):
        ExecutionSystem.__init__(self, name)
        self.native_specifications = native_specifications
    
    def run(self, job):
        """
        run a job asynchronously on the local system.
        
        :param job: the job to run.
        :type job: :class:`mobyle.common.job.Job` object.
        :returns: the pid of the job_script
        :rtype: int
        """
        job_dir = os.path.normpath(job.dir)
        if os.getcwd() != job_dir:
            msg = "job {id} is not in right dir: {cwd} instead of {job_dir}".format(id = job.id, cwd = os.getcwd(), job_dir = job_dir)
            self._log.critical(msg)
            raise InternalError(message = msg)
        service_name = job.service.name
        with open(os.path.join(job_dir, service_name + '.out'), 'w') as fout:
            with open(os.path.join(job_dir, service_name + '.err'), 'w') as ferr:
                try:
                    # the new process launch by popen must be a session leader
                    # because the pid store in job is the pid of the wrapper
                    # when we want to kill a job if we kill speciffically the pid of the wrapper
                    # the command is still running
                    # So to kill all (the wrapper and all cmde and subcmnd
                    # we need to kill the group
                    # So we need that the wrapper become a session leader
                    
                    setsid_path = which('setsid')
                    if setsid_path is None:
                        msg = 'no setsid in '+ os.environ["PATH"]
                        self._log.critical(msg)
                        raise InternalError(message = msg)
                    
                    job_wrapper_path =  os.path.join( job_dir , ".job_script" )
                    pipe = Popen([setsid_path, setsid_path, '/bin/sh', job_wrapper_path], 
                                 stdout = fout, 
                                 stderr = ferr, 
                                 shell = False,
                                 close_fds = True,
                                 env = job.cmd_env)
                except OSError as err:
                    msg= "Local execution failed: job dir = {job_dir} : {err}".format(job_dir = job_dir, err = err)
                    self._log.critical(msg, exc_info = True)
                    raise InternalError(message = msg) 
        return pipe.pid
    
    
    def get_status(self, job):
        """
        query the execution system to get the status of a job
        translate it in Mobyle Status **and update** the job
        
        :param job: the job to query the status
        :type job: :class:`mobyle.common.job.Job` object.
        :return: the status of the job.
        :rtype: :class:`mobyle.common.job.Status` object.
        """
        # the pid is the pid of the job_wrapper
        # it does not really matter as if the 
        # wrapper is alive the job should be too
        job_pid = int(job.execution_job_no)
        job_dir = os.path.normpath(job.dir)
        try:
            os.kill(job_pid, SIG_DFL)
        except OSError as err:
            if str(err).find('No such process') != -1:
                #the process is ended
                #what is the retun value?
                with open(os.path.join(job_dir,'.job_return_value'), 'r') as return_file:
                    return_code = return_file.readline()
                try:
                    return_code = int(return_code)
                except Exception as err:
                    msg = "cannot read job return value for {job_dir}: {err}".format(job_dir = job_dir, err = err)
                    self._log.error(msg, exc_info = True)
                    new_state = Status.UNKNOWN
                if return_code == 0:
                    new_state = Status.FINISHED
                else:    
                    new_state = Status.ERROR
            else:
                msg = "an unexpected error occured during querying a local job status: {job_dir} : {err}".format(job_dir = job_dir, err = err)
                self._log.error(msg, exc_info = True)
                new_state = Status.UNKNOWN
        else:    
            new_state = Status.RUNNING
        return Status(new_state)
    
    
    def kill(self, job):
        """
        ask to the execution to terminate a job, and updated it.
        
        :param job: the job to kill.
        :type job: :class:`mobyle.common.job.Job` object.
        """
        job_pid = int(job.execution_job_no)
        job_pgid = os.getpgid(job_pid)
        job_id = job.id
        try:
            os.killpg(job_pgid, SIGTERM)
            status = Status.KILLED
            job.status.state = status
            job.message("this job has been killed")
            job.save()
        except OSError as err:
            raise InternalError("cannot kill job {job_id}: {err}".format(job_id = job_id, err = err))
        try:
            sleep(0.2)
            #this kill should fail 
            #if not try kill -9 :-(
            os.killpg(job_pgid, SIG_DFL)
            try:
                os.killpg(job_pgid, SIGKILL)
            except OSError as err:
                raise InternalError("cannot kill job {job_id}: {err}".format(job_id = job_id, err = err))
        except OSError as err:
            return None

        