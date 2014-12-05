# -*- coding: utf-8 -*-

#========================
# :Date:Aug 19, 2014
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================
  

import logging
_log = logging.getLogger(__name__)

import os
import re

from mobyle.common.data import ListData
from mobyle.common.error import InternalError, UserValueError
from mobyle.common.eval_bool import EvalBoolFactory


class JobLogger(object):
    """
    JobLogger is a special logger which log information about:
    
    * the command line building
    * the recovering of outputs
    
    """
    
    def __init__(self, file_name):
        self.file_name = file_name
        self.name = 'build'
        self.stream = None
        self.log = None
        #self.formater = logging.Formatter('%(levelname)-8s : %(message)s')
        self.formater = logging.Formatter('%(message)s')
        ####################### BOUCHON ####################
        # aller dans la conf general de mobyle pour savoir quel est le niveau de debug de ce service
        # si pas defini => error
        self.level = logging.DEBUG
    
    
    def __enter__(self):
        try:
            self.stream = open(self.file_name, 'a')
        except IOError as err:
            msg = "unable to create file {} for build_log: {}".format(self.file_name, err)
            _log.critical(msg)
            raise InternalError("Server Internal Error")
        if self.log is None:
            self.log = logging.getLogger(self.name)
            self.log.setLevel(1)
            self.log.propagate = False
        handler = logging.StreamHandler(self.stream)
        handler.setFormatter(self.formater)
        handler.setLevel(self.level)
        self.log.addHandler(handler)
        return self.log

    def __exit__(self, exception_type, exc, trace_back):
        self.log.handlers[0].flush()
        self.log.removeHandler(self.log.handlers[0])
        self.stream.close()
        if trace_back is None:
            return True
        #by default re-raise exc except if return True



class Evaluator(object):
    """
    provide a virgin namespace to evaluate the python expression specified in service description
    without any interactions with mobyle internal variables
    """

    def __init__(self, program_job, log_file):
        """
        :param program_job: the program job to evaluate code
        :type program_job: :class:`mobyle.common.job.ProgramJob` instance
        :param log_file: the name of the file to store the logs.
        :type log_file: string
        """
        self.job = program_job
        self._job_log_file_name = os.path.join(self.job.dir, log_file) 
        self._evaluator = {}
        self._evaluator['re'] = re
        self._fill_evaluator(self._evaluator)
        
        
    def _pre_process_data(self, data):
        """
        :param data: the data to get the value 
        :type data: :class:`mobyle.common.data.AbstractData` instance or None
        :return: the value of a data ready to put it in the evaluator
        :rtype: any 
        """
        if data is None:
            value = None
        elif isinstance(data, ListData):
            value = data.expr_value()
            value.sort()
        else:
            #RefData, ValueData, StructData
            value = data.expr_value()
        return value
    
    
    def _fill_evaluator(self, evaluator):
        """
        fill the evaluator with the data value 
        
        :param evaluator: the evaluator to fill
        :type evaluator: :class:`Evaluator` object
        """
        with JobLogger(self._job_log_file_name) as job_log:
            job_log.error('##################\n# fill evaluator #\n##################')
            program = self.job.service
            for parameter in program.parameters_list():
                job_log.debug("------ parameter {0} ------".format(parameter.name))
                value_data = self.job.get_exec_input_value(parameter.name)
                #job_log.debug(parameter.name, value_data)
                job_log.debug("value_data = {0}".format(value_data))
                if value_data is None:
                    # if there is no vdef default_value return None
                    value_data = parameter.default_value
                value = self._pre_process_data(value_data)
                job_log.debug('{0} = {1}'.format(parameter.name, value))
                evaluator[parameter.name] = value
            job_log.debug("evaluator = {0}".format(evaluator))
    
    
    def eval_precond(self, preconds, job_log):
        eval_bool = EvalBoolFactory(values = self._evaluator)
        all_preconds_true = True
        for precond in preconds:
            try:
                evaluated_precond = eval_bool.test(precond)
                job_log.debug("precond = '{0}' => {1}".format(precond, evaluated_precond))
            except Exception as err:
                msg = "ERROR during precond evaluation: {0} : {1}".format(precond, err)
                _log.error(msg)
                job_log.debug(msg)
                raise InternalError("Internal Server Error")
            job_log.debug("precond = {0} evaluated as  {1}".format(precond, evaluated_precond))
            if not evaluated_precond :
                all_preconds_true = False
                break
        return all_preconds_true
            



class CommandBuilder(Evaluator):
    """provide a virgin namespace to evaluate the python expression specified in service description
    without any interactions with mobyle internal variables.
    provide all methods needed to build the command line en environemnt needed to excecute a ProgramJob
    """
        
    def check_ctrl(self):
        """
        :return: True  if all control parameter are True, else raise a UserValueError
        :rtype: boolean
        :raise UserValueError: if some ctrl failed
        """
        eval_bool = EvalBoolFactory(values = self._evaluator)
        program = self.job.service
        with JobLogger(self._job_log_file_name) as build_log:
            build_log.debug('##################\n# check control #\n##################') 
            for parameter in program.parameters_list():
                if parameter.has_ctrls():
                    preconds = parameter.preconds
                    all_preconds_true = self.eval_precond(preconds, build_log)
                    if all_preconds_true:
                        ctrls = parameter.ctrls
                        for ctrl in ctrls:
                            evaluated_ctrl = eval_bool.test(ctrl)
                            build_log.debug("ctrl = '{0}' => {1}".format(ctrl, evaluated_ctrl))
                            if not evaluated_ctrl:
                                msg = '{0} : value provided {1}'.format()
                                _log.error(msg)
                                build_log.debug(msg)
                                raise UserValueError(parameters = [parameter], message = msg)
                    else:
                        return True
                          
                          
    def check_mandatory(self):
        """
        :return: True, if all mandatory parameters have a value (check preconds if necessary).
        :rtype: boolean
        :raise UserValueError: if some mandatory parameter have no value
        """
        program = self.job.service
        param_missing_value = []
        with JobLogger(self._job_log_file_name) as build_log:
            build_log.debug('###################\n# check mandatory #\n###################')
            for parameter in program.inputs_list():
                build_log.debug("------ parameter {0} ------".format(parameter.name))
                if not parameter.mandatory:
                    build_log.debug(" not mandatory => next parameter")
                    continue
                build_log.debug("check parameter {0}".format(parameter.name))
                build_log.debug("------ parameter {0} ------".format(parameter.name))
                preconds = parameter.preconds
                all_preconds_true = self.eval_precond(preconds, build_log)    
                if not all_preconds_true :
                    build_log.debug("all preconds are not True: next parameter")
                    continue #next parameter
                if self._evaluator[parameter.name] is None:
                    param_missing_value.append(parameter)
                    build_log.debug("add parameter {0} in param_missing_value".format(parameter.name))
            if param_missing_value:
                message = "mandatory parameters missing value: {}".format([p.name for p in param_missing_value])
                build_log.debug(message)
                raise UserValueError(parameters = param_missing_value, message = message)
        return True    
            
                  
    def build_command(self):
        """
        evaluate the parameters format to build the command line and/or the configuration files
        
        :return: the command line
        :rtype: string 
        :raise InternalError: if something goes wrong during 'format' evaluation.
        """
        program = self.job.service
        command_line = ''
        paramfile_handles = {}
        
        def close_paramfiles():
            """
            close all open paramfiles 
            """
            for handle in paramfile_handles:
                try:
                    handle.close()
                    self.debug.debug
                except IOError as err:
                    msg = "cannot close paramfile {0}: {1}".format(handle.name, err)
                    _log.error(msg)
                
        with JobLogger(self._job_log_file_name) as build_log:
            build_log.debug('#################\n# build command #\n#################')
            command_is_insert = False
            for parameter in program.parameters_list_by_argpos():
                build_log.debug("------ parameter {0} ------".format(parameter.name))
                arg_pos = parameter.argpos
                build_log.debug("arg_pos = {}  command_is_insert = {}".format(arg_pos, command_is_insert))
                if arg_pos >= 0 and not command_is_insert:
                    command = program.command
                    if command is not None:
                        command_line += command
                        command_is_insert = True
                        build_log.debug("insert command : command_line = {0}".format(command_line))
                    else:
                        if parameter.command:
                            command_is_insert =True
                        else:
                            msg = 'the parameter {0}.{1} have argpos {2} and no command found'.format(program.name, parameter.name, arg_pos)
                            _log.error(msg)
                            build_log.error(msg)
                            raise InternalError(msg)
                 
                vdef_data = parameter.default_value
                vdef = self._pre_process_data(vdef_data)
                self._evaluator['vdef'] = vdef
                build_log.debug("vdef = {0}".format(vdef))
                value = self._evaluator[parameter.name]
                self._evaluator['value'] = value
                build_log.debug("value = {0}".format(value))
                try:
                    preconds = parameter.preconds
                except InternalError, err:
                    close_paramfiles()
                    raise
                all_preconds_true = self.eval_precond(preconds, build_log)    
                if not all_preconds_true :
                    build_log.debug("all preconds are not True: next parameter")
                    continue #next parameter
                
                if parameter.has_format():
                    format_ = parameter.format
                    try:
                        build_log.debug("format = {0} , type = {1}".format(format_, type(format_)))
                        cmd_chunk = eval(format_, self._evaluator)
                        build_log.debug("cmd_chunk = {0}".format(cmd_chunk))
                    except Exception as err:
                        msg = "ERROR during evaluation of program {0}: parameter {1} : format {2} err {3}".format(program['name'],
                                                                                                                  parameter.name,
                                                                                                                  format_,
                                                                                                                  err)
                        _log.critical(msg, exc_info = True)
                        build_log.error(msg)
                        close_paramfiles()
                        raise InternalError(msg)
                    
                    if parameter.has_paramfile():
                        paramfile_name = parameter.paramfile
                        try:
                            paramfile_handle = paramfile_handles.get(paramfile_name, open(paramfile_name, 'w'))
                        except IOError as err:
                            msg = 'cannot open paramfile {0} : {1}'.format(paramfile_name, err)
                            _log.critical(msg)
                            build_log.error(msg)
                            close_paramfiles()
                            raise InternalError(msg)
                        
                        if cmd_chunk :
                            paramfile_handle.write(cmd_chunk)
                            paramfile_handle.flush()
                            build_log.debug('write "{0}" in paramfile : {1}'.format(cmd_chunk, paramfile_handle.name))
                    else:
                        command_line += str(cmd_chunk) 
                        build_log.debug("command line = {0}".format(command_line))
            
            close_paramfiles()
                
            #trim multi espaces tab , ...
            command_line = ' '.join(command_line.split())
            command_line.strip()
            command_line = command_line.replace('"','\\"')
            command_line = command_line.replace('@','\@')
            command_line = command_line.replace('_SQ_',"\'")
            command_line = command_line.replace('_DQ_','\"')
            build_log.debug("command line return = {0}".format(command_line))
        return command_line


    def build_env(self):
        """
        :return: the environment needed by the job to be executed 
        :rtype: dict
        """
        with JobLogger(self._job_log_file_name) as build_log:
            build_log.debug('##################\n# build env #\n##################')
            program = self.job.service
            job_env = program.env.copy()
            if 'PATH' in job_env:
                job_env['PATH'] = '{0}:{1}'.format(job_env['PATH'], os.environ['PATH'])
        return job_env
        
