# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from execution_system import ExecutionSystem, register
from subprocess import Popen
import os
from mobyle.common.job import Status

@register
class Local(ExecutionSystem):
    
    def __init__(self, name, native_specifications = ""):
        ExecutionSystem.__init__(self, name)
        self.native_specifications = native_specifications
    
    def run(self, job):
        """
        run a job asynchronously on the execution system.

        :param job: the job to run.
        :type job: :class:`mobyle.common.job.Job` object.
        """
        os.chdir(job.dir)
        out = open( os.path.join(job.dir, "_stdout") , 'w' )
        err = open( os.path.join(job.dir, "_stderr") , 'w' )
        pipe = Popen(['/bin/sh', '.command'], stdout=out, stderr=err, shell=False)
        job.execution_job_no = str(pipe.pid)
    
    def get_status(self, job):
        try:
            os.kill(int(job.execution_job_no), 0)
            job.status.state = Status.RUNNING
        except OSError as err:
            job.status.state = Status.FINISHED
        pass
    
    def kill(self, job):
        pass
