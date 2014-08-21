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
from mobyle.common.mobyleError import MobyleError, UserValueError

class BuildLogger(object):
    
    def __init__(self, file_name):
        self.file_name = file_name
        self.name = 'build'
        self.stream = None
        self.log = None
        self.formater = logging.Formatter('%(levelname)-8s : %(message)s')
        ####################### BOUCHON ####################
        # aller dans la conf general de mobyle pour savoir quel est le niveau de debug de ce service
        # si pas defini => error
        self.Level = 0
    
    def __enter__(self, file_name):
        self.stream =  open(self.file_name, 'a')
        if self.log is None:
            self.log = logging.getLogger(self.name)
            self.log.setLevel(self.level)
        handler = logging.StreamHandler(self.stream)
        handler.setFormatter(self.formater)
        self.log.addHandler(handler)
        return self.log

    def __exit__(self, exception_type, exc, trace_back):
        self.log.handlers[0].flush()
        self.log.removeHandler(self.log.handlers[0])
        self.stream.close()
        if trace_back is None:
            return True
        #by default re-raise exc except if return True


class CommandBuilder(object):
    
    def __init__(self, program_job):
        self.job = program_job
        self._evaluator = {}
        self._evaluator['re'] = re
        self._pre_fill_evaluator(self._evaluator)
        self._build_log_file_name = os.path.join(self.job.dir, '.cmd_build_log')
    
    def _pre_process_data(self, data):
        """
        :param data: the data to get the value 
        :type data: :class:`mobyle.common.data.AbstractData` instance or None
        :return: the value of a data ready to put it in the evaluator
        :rtype: any 
        """
        if data is None:
            value = None
        elif isinstance(ListData, data):
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
        with BuildLogger(self._build_log_file_name) as build_log:
            build_log.debug('##################\n# fill evaluator #\n##################')
            program = self.job.service
            for parameter in program.inputs_list():
                value_data = self.job.get_input_value(parameter.name)
                if value_data is None:
                    # if there is no vdef default_value return None
                    value_data = parameter.default_value()
                value = self._pre_process_data(value_data)
                self._cl_log.debug('{} = {}'.format(parameter.name, value))
                evaluator[parameter.name] = value
            build_log.debug("evaluator = {0}".format(evaluator))
            
    
    def _eval_precond(self, preconds, build_log):
        all_preconds_true = True
        for precond in preconds:
            try:
                evaluated_precond = eval(precond, self._evaluator)
                build_log.debug("precond = '{0}' => {1}".format(precond, evaluated_precond))
            except Exception as err:
                msg = "ERROR during precond evaluation: {0} : {1}".format(precond, err)
                _log.error(msg)
                build_log.debug(msg)
                raise MobyleError("Internal Server Error")
            build_log.debug("precond = {0} evaluated as  {1}".format(precond, evaluated_precond))
            if not evaluated_precond :
                all_preconds_true = False
                break
        return all_preconds_true
        
        
        
    def check_mandatory(self):
        """
        :return: True, if all mandatory parameters have a value (check preconds if necessary).
        :rtype: boolean
        :raise UserValueError: if some mandatory parameter have no value
        """
        program = self.job.service
        param_missing_value = []
        with BuildLogger(self._build_log_file_name) as build_log:
            build_log.debug('##################\n# check mandatory #\n##################')
            for parameter in program.inputs_list_by_argpos():
                preconds = parameter.preconds()
                all_preconds_true = self._eval_precond(preconds, build_log)    
                if not all_preconds_true :
                    build_log.debug("next parameter")
                    continue #next parameter
                if self._evaluator[parameter.name] is None and parameter.mandatory:
                    param_missing_value.append(parameter)
                    build_log.debug("add parameter {0} in param_missing_value".format(parameter.name))
            if param_missing_value:
                build_log.debug("mandatory parameters missing value: {}".format(param_missing_value))
                raise UserValueError(parameters = param_missing_value, message = "parameter is mandatory")
        return True    
            
            
    def check_ctrl(self):
        """
        :return: True  if all control parameter are True, else raise a UserValueError
        :rtype: boolean
        :raise UserValueError: if some ctrl failed
        """
        program = self.job.service
        with BuildLogger(self._build_log_file_name) as build_log:
            build_log.debug('##################\n# check control #\n##################') 
            for parameter in program.inputs_list_by_argpos():
                if parameter.has_ctrl():
                    preconds = parameter.preconds()
                    all_preconds_true = self._eval_precond(preconds, build_log)
                    if all_preconds_true:
                        ctrls = parameter.get_ctrls()
                        for crtl in ctrls:
                            pass
                    else:
                        return True
                  
                  
    def build_command(self):
        """
        evaluate the parameters format to build the command line and/or the configuration files
        
        :return: the command line
        :rtype: string 
        :raise MobyleError: if something goes wrong during 'format' evaluation.
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
                
        with BuildLogger(self._build_log_file_name) as build_log:
            build_log.debug('##################\n# build command #\n##################')
            command_is_insert = False
            for parameter in program.inputs_list_by_argpos():
                arg_pos = parameter.argpos
                if arg_pos >= 0 and not command_is_insert:
                    command = program.command
                    if command is not None:
                        command_line += command
                        command_is_insert = True
                    else:
                        if parameter.command:
                            command_is_insert =True
                        else:
                            msg = 'the parameter {0}.{1} have argpos {2} and no command found'.format(program.name, parameter.name, arg_pos)
                            _log.error(msg)
                            build_log.error(msg)
                            raise MobyleError(msg)
                 
                vdef_data = parameter.default_value()
                vdef = self._pre_process_data(vdef_data)
                self._evaluator['vdef'] = vdef
                build_log.debug("vdef = {0}".format(vdef))
                value_data = self.job.get_input_value(parameter.name)
                value = self._pre_process_data(value_data)
                self._evaluator['value'] = value
                build_log.debug("value = {0}".format(value))
                try:
                    preconds = parameter.preconds()
                except MobyleError, err:
                    close_paramfiles()
                    raise
                all_preconds_true = self._eval_precond(preconds, build_log)    
                if not all_preconds_true :
                    build_log.debug("all preconds are not True: next parameter")
                    continue #next parameter
                
                if parameter.has_format():
                    format_ = parameter.format
                    try:
                        build_log.debug("format = {0}".format(format_))
                        cmd_chunk = eval(format, self._evaluator)
                        build_log.debug("cmd_chunk = {0}".format(cmd_chunk))
                    except Exception as err:
                        msg = "ERROR during evaluation of program {0}: parameter {1} : format {2} err {3}".format(program.name,
                                                                                                                  parameter.name,
                                                                                                                  format_,
                                                                                                                  err)
                        _log.critical(msg, exc_info = True)
                        build_log.error(msg)
                        close_paramfiles()
                        raise MobyleError(msg)
                    
                    if parameter.has_paramfile():
                        paramfile_name = parameter.paramfile
                        try:
                            paramfile_handle = paramfile_handles.get(paramfile_name, open(paramfile_name, 'w'))
                        except IOError as err:
                            msg = 'cannot open paramfile {0} : {1}'.format(paramfile_name, err)
                            _log.critical(msg)
                            build_log.error(msg)
                            close_paramfiles()
                            raise MobyleError(msg)
                        
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
            command_line.replace( '"','\\"' )
            command_line.replace( '@','\@' )
            command_line.replace( '_SQ_',"\'" )
            command_line.replace( '_DQ_','\"' )
            build_log.debug("command line return = {0}".format(command_line))
        return command_line


    def build_env(self):
        """
        :return: the environment needed by the job to be executed 
        :rtype: dict
        """
        with BuildLogger(self._build_log_file_name) as build_log:
            build_log.debug('##################\n# build env #\n##################')
            program = self.job.service
            job_env = program.env.copy()
            if 'PATH' in job_env:
                job_env['PATH'] = '{0}:{1}'.format(job_env['PATH'], os.environ['PATH'])
        return job_env
        