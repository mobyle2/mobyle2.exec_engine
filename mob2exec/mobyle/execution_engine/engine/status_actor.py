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
        old_status = job.status
        job.status.state = Status.UPDATING
        job.save()
        
        exec_system_id = str(job.execution_system_id)
        exec_system = self.get_execution_system(exec_system_id)
        
        new_status = exec_system.get_status(job)
        if old_status != new_status:
            if new_status.is_ended():
                job.end_time = datetime.now()
            if new_status == Status(Status.FINISHED):
                self.get_results(job)
            try:
                job.status.state =  new_status.state   
            except InternalError as err:
                msg = "problem to update status of job: {job_dir} : err".format(job_dir = job.dir, err = err)
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
        program = self.job.service
        evaluator = Evaluator(job, job.log_file_name)
        with JobLogger(job.log_file_name) as job_log:
            job_log.debug('###################\n# retrieve results #\n###################')
            for parameter in program.outputs_list():
                job_log.debug("------output parameter {0} ------".format(parameter.name))
                vdef_data = parameter.default_value
                vdef = self._pre_process_data(vdef_data)
                evaluator['vdef'] = vdef
                job_log.debug("vdef = {0}".format(vdef))
                value = evaluator[parameter.name]
                evaluator['value'] = value
                job_log.debug("value = {0}".format(value))
                preconds = parameter.preconds
                all_preconds_true = self.eval_precond(preconds, job_log)    
                if not all_preconds_true :
                    job_log.debug("all preconds are not True: next parameter")
                    continue #next parameter
                
                filenames = parameter.filenames()
                job_log.debug("filenames = {0}".format(filenames))
                unix_mask = None
                if filenames:
                    try:
                        unix_mask = eval(filenames, evaluator)
                        job_log.debug("unix_mask = {0}".format(unix_mask))
                    except Exception as err:
                        msg = "ERROR during evaluation of program {0}: parameter {1} : filenames {2} err {3}".format(program['name'],
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
                            data = RefData()
                            data['type'] = parameter.type
                            data['path'] = result_files[0]
                            data['size'] = os.path.getsize(result_files[0])
                        else:
                            data = ListData()
                            list_of_data = []
                            for one_file in result_files:
                                data = RefData()
                                data['type'] = parameter.type
                                data['path'] = one_file[0]
                                data['size'] = os.path.getsize(one_file[0])
                                list_of_data.append(data)
                            data['value'] = list_of_data
                        job['outputs'][parameter.name] = data
                    else:
                        continue
           
            