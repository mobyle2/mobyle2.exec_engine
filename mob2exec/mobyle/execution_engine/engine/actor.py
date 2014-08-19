# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 7, 2014
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================


import multiprocessing
import os
from abc import ABCMeta, abstractmethod

from mobyle.common.connection import connection
from mobyle.common.job import ProgramJob
from mobyle.common.job_routing_model import ExecutionSystem
from mobyle.common.mobyleError import MobyleError
from mobyle.execution_engine.systems.execution_system import load_execution_classes


class Actor(multiprocessing.Process):
    """
    Base class for all actors.
    """
    __metaclass__ =  ABCMeta

    def __init__(self, job_id, log_conf):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        
        """
        super(Actor, self).__init__()
        self.job_id = job_id
        self._log_conf =  log_conf  
        self._log = None
         
           
    def get_job(self): 
        """
        fetch the job corresponding to the jobid used to instanciate the actor from DB
        
        :returns: :class:`mobyle.common.job` object
        """       
        try:
            job = connection.Job.fetch_one({'_id' : self.job_id})
        except Exception, err:
            self._log.error(str(err), exc_info = True)
            # TO BE IMPROVE
            raise err
        if job is None:
            self._log.Error('try to fetch job id = {0} which was not found in DB'.format(self.job_id))        
        return job
    
    
    def get_execution_system(self, exec_name):
        """
        fetch the execution systems configuration corresponding to exec_name from DB
        
        :param exec_name: the name of the Execution system to fetch
        :type exec_name: string
        :returns: :class:`mobyle.executon_engine.systems.exceution_system.ExecutionSytem` object
        """  
        try:
            exec_conf = connection.ExecutionSystem.fetch_one({'_id' : exec_name})
        except Exception, err:
            self._log.error(str(err), exc_info = True)
            raise err
        exec_klasses = load_execution_classes()
        try:
            klass = exec_klasses[exec_conf["class"]]
        except KeyError, err:
            raise MobyleError('class {0} does not exist check your config'.format(exec_conf["class"]))
        opts = exec_conf["drm_options"]
        if opts is None:
            opts = {}
        native_specifications = exec_conf["native_specifications"]
        if native_specifications:
            opts["native_specifications"] = native_specifications
        try:
            execution_system = klass(exec_conf["_id"], **opts)
        except Exception, err:
            msg = 'cannot instantiate class {0} : {1}'.format(exec_conf["class"]), err
            self._log.error(msg)
            raise MobyleError(msg)
        
        return execution_system
        
    @abstractmethod   
    def run(self):
        pass
