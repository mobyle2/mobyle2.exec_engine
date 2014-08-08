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

import logging
_log = logging.getLogger(__name__)

from mobyle.common.connection import connection
from mobyle.common.config import Config
from mobyle.common.mobyleError import MobyleError

from .rules import load_rules 

class Rule(object):
    """is condition which can be ask """ 
    rules_reg = load_rules()
    
    def __init__(self, name, parameters = None):
        """
        :param name: the name of the rule
        :type name: string
        :param parameters: the extra parameters pass when the rule is invoked
        :type parameters: dict
        """
        self.name = name
        if parameters is None:
            parameters = {}
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
    
class Route(object):
    """
    a route is the association between a set of rules and an execution systems
    """
    
    def __init__(self, name, exec_sys, rules = None):
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
        if rules is None:
            rules = []
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


class Dispatcher(object):
    """
    the container for all routes
    """
    
    def __init__(self, routes = None):
        """
        :param routes: the set of routes
        :type routes: OrderedDict
        """
        if routes is None:
            routes = OrderedDict()
        self.routes = routes 

    def __getattr__(self, name):
        return getattr(self.routes, name)
    
    def __iter__(self):
        return iter(self.routes.values())
    
    def __len__(self):
        return len(self.routes)
    
    def append(self, route):
        """
        :param route: a route to appnd to this dispatcher
        :type route: c'ass:`Route` instance
        
        """
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
            
            
def get_dispatcher():
    from mobyle.execution_engine.systems.execution_system import load_execution_classes
    from mobyle.common.job_routing_model import ExecutionSystem
    exec_klass = load_execution_classes()
    exec_systems = {}

    all_exec_in_conf = connection.ExecutionSystem.fetch({})
    for exec_conf in all_exec_in_conf:
        try:
            klass = exec_klass[exec_conf["class"]]
        except KeyError, err:
            raise MobyleError('class {0} does not exist check your config'.format(exec_conf["class"]))
        opts = exec_conf["drm_options"]
        if opts is None:
            opts = {}
        native_specifications = exec_conf["native_specifications"]
        if native_specifications:
            opts["native_specifications"] = native_specifications
        try:
            exec_systems[exec_conf["_id"]] = klass(exec_conf["_id"], **opts)
        except Exception, err:
            msg = 'cannot instantiate class {0} : {1}'.format(exec_conf["class"]), err
            _log.error(msg)
            raise MobyleError(msg)
    dispatcher = Dispatcher()
    
    map_ = connection.ExecutionRoutes.fetch_one({})["map"]
    for route_conf in map_:
        rules = []
        for rule_conf in route_conf["rules"]:
            parameters = rule_conf.get("parameters", {})
            rule = Rule(rule_conf["name"], parameters = parameters)
            rules.append(rule)
        exec_sys = exec_systems[route_conf["exec_system"]]
        route = Route(route_conf["name"], exec_sys, rules )
        dispatcher.append(route)
    return dispatcher


