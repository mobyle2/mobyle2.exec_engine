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
    
    def __init__(self, routes = OrderedDict()):
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
            
            
def _get_dispatcher():
    from mobyle.execution_engine.systems.execution_system import load_execution_classes
    conf = { "execution_systems" : [{"name" : "big_one",
                              "class" : "OgsDRMAA",
                              "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                               "cell" : '/usr/local/sge',
                                               "root" : 'default', 
                                               },
                                "native_specifications": " -q mobyle-long " 
                                },
                                {"name" : "small_one",
                                 "class" : "OgsDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                                  "cell" : '/usr/local/sge',
                                                  "root" : 'default' 
                                                  },
                                 "native-options": " -q mobyle-small " 
                                 },
                                {"name" : "cluster_two",
                                 "class" : "TorqueDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/torque/libdrmaa.so",
                                                  "server_name" : "localhost" 
                                                  },
                                 "native_specifications": " -q mobyle-small " 
                                 },
                                {"name" : "local",
                                 "class" : "Local",
                                 "native_specifications" : " nice -n 18 "
                                 }],
            
                "map": [ ("route_1", {"rules" : [{"name" : "user_is_local"} , {"name" : "job_name_match", 
                                                                              "parameters" : {"name": "Filochard"}}],
                                      "exec_sys" : "big_one" 
                                      }),
                         ("route_2", {"rules" : [{"name" : "project_match",
                                                  "parameters" : {"name": "dans le cambouis"}} ],
                                      "exec_sys" : "small_one" 
                                      }),
                         ("default", {"rules" : [],
                                      "exec_sys" : "cluster_two" 
                                      })
                        ]
               } 
    exec_klass = load_execution_classes()
    exec_systems = {}
    for exec_conf in conf["execution_systems"]:
        try:
            klass = exec_klass[exec_conf["class"]]
        except KeyError, err:
            raise MobyleError('class {0} does not exist check your config'.format(exec_conf["class"]))
        opts = exec_conf["drm_options"] if "drm_options" in exec_conf else {}
        opts.update({"native_specifications" : exec_conf["native_specifications"]} if "native_specifications" in exec_conf else {})
        try:
            exec_systems[exec_conf["name"]] = klass( exec_conf["name"], **opts )
        except Exception, err:
            print exec_conf["name"]
            print opts
            print err       
    dispatcher = Dispatcher()

    for route_conf in conf["map"]:
        rules = []
        for rule_conf in route_conf[1]["rules"]:
            rule = Rule(rule_conf["name"], parameters = rule_conf["parameters"] if "parameters" in rule_conf else {})
            rules.append(rule)
        exec_sys = exec_systems[route_conf[1]["exec_sys"]]
        route = Route(route_conf[0], exec_sys, rules )
        dispatcher.append(route)
    return dispatcher


dispatcher = _get_dispatcher()
