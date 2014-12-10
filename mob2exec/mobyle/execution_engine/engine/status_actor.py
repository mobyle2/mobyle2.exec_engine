# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import os.path
import logging.config
import setproctitle
import glob

from mobyle.common.data import RefData, ListData
from mobyle.common.job import Status
from mobyle.common.error import InternalError
from mobyle.execution_engine.evaluator import Evaluator, JobLogger

from .actor import Actor

####### BOUCHON ##########
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
        self._log = logging.getLogger(__name__) 
        
        job = self.get_job()
        #we must generate a new status object not just change the state
        #other wise old_statuschange each time we change the job_status.state
        #as it is the same object
        old_status = Status(job.status.state)
        job.status.state = Status.UPDATING
        job.save()
        self._log.info( "{0} try to get status of job {1} actual status is {2}".format(self._name, job.id, old_status))
        try:
            os.chdir(job.dir)
        except OSError as err:
            msg = "cannot change working dir for job dir : {0}".format(err)
            self._log.critical(msg)
            job.status.state = Status.ERROR
            job.save()
            raise InternalError(message = msg)
        
        exec_system_id = str(job.execution_system_id)
        exec_system = self.get_execution_system(exec_system_id)
        
        new_status = exec_system.get_status(job)
        if old_status != new_status:
            if new_status.is_ended():
                if os.path.exists(job.return_value_file):
                    timestamp = os.path.getmtime(job.return_value_file)
                    job.end_time = datetime.fromtimestamp(timestamp)
                else:
                    job.end_time = datetime.now()
            if new_status == Status(Status.FINISHED):
                self.get_results(job)
            try:
                job.status.state =  new_status.state   
            except InternalError as err:
                msg = "problem to update status of job: {job_dir} : {err}".format(job_dir = job.dir, err = err)
                self._log.error(msg)
                raise
            finally:
                job.save()
        job.save()
        self._log.debug("{0} exiting".format(self._name))
        
        
    def get_results(self, job):
        """
        :todo: we assume we have only **ONE** file. to take in account directory we need to recode.  
        """
        program = job.service
        evaluator = Evaluator(job, job.log_file_name)
        with JobLogger(job.log_file_name) as job_log:
            job_log.debug('###################\n# retrieve results #\n###################')
            for parameter in program.outputs_list():
                job_log.debug("------output parameter {0} ------".format(parameter.name))
                vdef_data = parameter.default_value
                vdef = evaluator.pre_process_data(vdef_data)
                evaluator['vdef'] = vdef
                job_log.debug("vdef = {0}".format(vdef))
                value = evaluator[parameter.name]
                evaluator['value'] = value
                job_log.debug("value = {0}".format(value))
                preconds = parameter.preconds
                all_preconds_true = evaluator.eval_precond(preconds, job_log)    
                if not all_preconds_true :
                    job_log.debug("all preconds are not True: next parameter")
                    continue #next parameter
                
                #filenames must be One string for parameter
                filenames = parameter.filenames
                job_log.debug("filenames = {0}".format(filenames))
                unix_mask = None
                if filenames:
                    try:
                        unix_mask = evaluator.eval(filenames)
                        job_log.debug("unix_mask = {0}".format(unix_mask))
                    except Exception as err:
                        msg = "ERROR during evaluation of parameter {0}.{1} : filenames {2}: err {3}".format(program['name'],
                                                                                                                     parameter.name,
                                                                                                                     filenames,
                                                                                                                     err)
                        self._log.critical(msg, exc_info = True)
                        job_log.error(msg)
                        raise InternalError(msg)
                if unix_mask:
                    result_files = glob.glob(unix_mask)
                    job_log.debug("mask match = {0}".format(result_files))
                    if result_files:
                        if len(result_files) == 1:
                            size = os.path.getsize(result_files[0])
                            if size != 0:
                                data = RefData()
                                data['type'] = parameter.type
                                data['path'] = result_files[0]
                                data['size'] = size
                        else:
                            data = ListData()
                            list_of_data = []
                            for one_file in result_files:
                                size = os.path.getsize(one_file)
                                if size != 0:
                                    data = RefData()
                                    data['type'] = parameter.type
                                    data['path'] = one_file
                                    data['size'] = size
                                    list_of_data.append(data)
                            data['value'] = list_of_data
                        job['outputs'][parameter.name] = data
                    else:
                        continue
            job.save()
                 
           
            