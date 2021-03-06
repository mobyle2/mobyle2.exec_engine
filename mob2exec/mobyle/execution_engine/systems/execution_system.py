# -*- coding: utf-8 -*-

#========================
# :Date:Aug 27, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from abc import ABCMeta, abstractmethod
import inspect
import sys
import os
import glob

from mobyle.common.error import InternalError

class ExecutionSystem(object):
    
    __metaclass__ =  ABCMeta
    
    def __init__(self, name):
        self.klass_name = self.__class__.__name__[:-6]
        self.name = name
        
    @abstractmethod   
    def run(self, job):
        """
        run a job asynchronously on the execution system.
        :param job: the job to run.
        :type job: :class:`mobyle.common.job.Job` object.
        """
        pass
    
    @abstractmethod 
    def get_status(self, job):
        """
        query the execution system to get the status of a job
        translate it in Mobyle Status and update the DB
        
        :param job: the job to query the status
        :type job: :class:`mobyle.common.job.Job` object.
        :return: the status of the job.
        :rtype: :class:`mobyle.common.job.Status` object.
        """
        pass
    
    @abstractmethod 
    def kill(self, job):
        """
        ask to the execution to terminate a job, and updated it in DB
        
        :param job: the job to kill.
        :type job: :class:`mobyle.common.job.Job` object.
        """
        pass
    

def make_register():  
    """
    closure to embed klass_registery, module_imported data structure
    in function register and load_execution_classes but avoid to
    expose them at the global level
    """
      
    klass_registery = {}
    module_imported = {}

    def register(klass):
        """
        decorator which register a class as an available ExcutionSystem class
        
        ..note ::
          from execution_system import ExecutionSystem
          
          @execution_system.register
          class alacon(ExecutionSystem):
               pass
        
        to be registered a class must inherits, directly or indirectly, from ExcutionSystem
        and implements a least 3 methods:
        
        * run(self, job)
        * get_status(self, job)
        * kill(self, job)
        
        """
        if not issubclass(klass, ExecutionSystem):
            raise InternalError("{0} does not inherits from ExecutionSystem".format(klass.__name__))
        if inspect.isabstract(klass):
            raise InternalError("{0} is still abstract, it must implements run, get_status, kill")
        klass_registery[klass.__name__] = klass
        return klass
        

    def load_execution_classes():
        """ 
        discover all modules in execution_systems package and load ExecutionSystems defined in them.
        
        if load_execution_classes is called twice, new modules are taken in account, but the
        deleted module or modification of module are ignored. 
        
        :return: the registery of ExecutionSystems classes
        :rtype: dict
        """
        def load(path):
            sys.path.insert(0, path)
            for f in glob.glob(os.path.join(path, '*.py')):
                module_name = os.path.splitext( os.path.basename(f))[0]
                if module_name != '__init__':
                    if module_name in module_imported:
                        pass
                    else:
                        module = __import__(module_name, globals(), locals(), [module_name])
                        module_imported[module_name] = module
            #clean the sys.path to avoid name collision
            sys.path.pop(0)
            
        mobyle_execution_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
        load(mobyle_execution_path)
        user_execution_path = os.path.abspath(os.path.join( os.path.dirname(__file__), '..', '..', '..', 'execution_systems'))
        load(user_execution_path)
        return klass_registery.copy()
    return register, load_execution_classes
    
    
register, load_execution_classes = make_register()    
    



        

