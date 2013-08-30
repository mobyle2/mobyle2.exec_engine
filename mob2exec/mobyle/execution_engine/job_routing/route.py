# -*- coding: utf-8 -*-

#========================
# :Date:Aug 26, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================
  
from collections import OrderedDict
from mongokit import CustomType

from mobyle.common.connection import connection
from mobyle.common.config import Config
from mobyle.common.mobyleError import MobyleError

from .rules import load_rules 

class Rule(object):
    """is condition which can be ask """ 
    rules_reg = load_rules()
    
    def __init__(self, name, parameters = {}):
        """
        :param name: the name of the rule
        :type name: string
        :param parameters: the extra parameters pass when the rule is invoked
        :type parameters: dict
        """
        self.name = name
        self.parameters = parameters
        try:
            self.func = Rule.rules_reg[self.name]
        except KeyError:
            raise MobyleError("no rule named : {0}".format(self.name))
        
    def __call__(self, job):
        """
        execute the function with the same name of this rule and pass the job as fisrt argument
        and the parameters specified in the config 
        :param job: the job test the rule against
        :type job: :class:`mobyle.common.job.Job` instance
        :return: True if the job pass the rule, False otherwise 
        :rtype: bool 
        """
        f = self.func
        return f(job, **self.parameters)
    
        
# class CustomRule(CustomType):
#     
#     mongo_type = unicode
#     init_type = Rule
#     
#         
#     def to_bson(self, value):
#         return {'name' : value.name,  
#                 'parameters' : value.kwargs
#                 }
#     
#     def to_python(self, value):
#         return Rule(value['name'], parameters = value['parameters'])
#     


class Route(object):
    """
    a route is the association between a set of rules and an execution systems
    """
    
    def __init__(self, name, exec_sys, rules = []):
        """
        :param name:
        :type name: string
        :param exec_sys: the execution system attached to this set of rules
        :type exec_sys: :class:`mobyle.executon_engine.systems.exceution_system.ExecutionSytem` object
        :param rules: the rules 
        :type rules: list of :class:`Rule` objects
        """
        self.name = name
        self._exec_sys = exec_sys
        self.rules = rules
    
    
    @property
    def exec_sys(self):
        return self._exec_sys
    
    
    def append(self, rule):
        """
        append a rule at the end of the set of rules
        :param rule: the rule to append
        :type rule: a :class:`Rule` object 
        """
        self.rules.append(rule)
    
    
    def allow(self, job):
        """
        :return: True if the job match all the rules. False otherwise.
        :rtype: bool
        """
        for rule in self.rules:
            res = rule(job)
            if not res:
                return False
        return True
    

# class CustomRoute(CustomType):
#     
#     mongo_type = unicode
#     init_type = Route
#     
#         
#     def to_bson(self, value):
#         return {'name' : value.name,  
#                 'rules': [CustomRule().to_bson(r) for r in value.rules ],
#                 'parameters' : value.kwargs
#                 }
#     
#     def to_python(self, value):
#         return Route(value['name'], 
#                      exec_sys =  value['exec_sys'],
#                      rules = value['rules'])
        
        
        
class Dispatcher(object):
    """
    the container for all routes
    """
    
    def __init__(self, routes = OrderedDict()):
        self.routes = routes 

    def __getattr__(self, name):
        return getattr(self.routes, name)
    
    def __iter__(self):
        return iter(self.routes.values())
    
    def __len__(self):
        return len(self.routes)
    
    def append(self, route):
        self.routes[route.name] = route
    
    def which_route(self,job):
        """
        :param job: the job to route
        :type job: class:`mobyle.common.job.Job` object
        :return: the first route which job satisfied all rules
        :rtype: class:`Route` instance
        """
        for route in self.routes.values():
            if route.allow(job):
                return route 
            
            
