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

from mobyle.common.mobyleError import MobyleError

class ExecutionSystem(object):
    
    __metaclass__ =  ABCMeta
    
    def __init__(self, name):
        self.klass_name = self.__class__.__name__[:-6]
        self.name = name
        
    @abstractmethod   
    def run(self):
        pass
    
    @abstractmethod 
    def get_status(self, job):
        pass
    
    @abstractmethod 
    def kill(self, job):
        pass
    
    
_class_registery = {}


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
        raise MobyleError("{0} does not inherits from ExecutionSystem".format(klass.__name__))
    if inspect.isabstract(klass):
        raise MobyleError("{0} is still abstract, it must implements run, get_status, kill")
    _class_registery[klass.__name__] = klass
    return klass
    



def load_execution_classes():
    """ 
    discover all modules in execution_systems package and load ExecutionSystems defined in them.
    :return: the registery of ExecutionSystems classes
    :rtype: dict
    """
    global _class_registery
    if _class_registery:
        _class_registery= {}
    
    def load(path):
        sys.path.insert(0, path)
        for f in glob.glob(os.path.join(path, '*.py')):
            module_name = os.path.splitext( os.path.basename(f))[0]
            if module_name != '__init__':
                __import__(module_name, globals(), locals(), [module_name])
        #clean the sys.path to avoid name collision
        sys.path.pop(0)
        
    mobyle_execution_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    load(mobyle_execution_path)
    user_execution_path = os.path.abspath(os.path.join( os.path.dirname(__file__), '..', '..', '..', 'execution_systems'))
    load(user_execution_path)
    return _class_registery
    
    
    

    
    



        

