# -*- coding: utf-8 -*-

#==============================================
#Created on Aug 27, 2012
#
#@author: Bertrand Néron
#@contact: bneron@pasteur.fr
#@organization: Institut Pasteur
#@license: GPLv3
#==============================================

import os
import unittest

#a config object must be instantiated first for each entry point of the application
#a config object must be instantiated first for each entry point of the application
from mobyle.common.config import Config
config = Config( os.path.join( os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection

from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status, ProgramJob, Job
from mobyle.common.job_routing_model import ExecutionRoutes

from mobyle.common.mobyleError import MobyleError
from mobyle.execution_engine.job_routing.route  import Rule

class RuleTest(unittest.TestCase):

    def setUp(self):
        def always_true(job):
            return True
        
        def always_false(job):
            return False
        
        def normand(job, resp = None):
            return resp
        
        Rule.rules_reg = { "always_true" : always_true,
                           "always_false" : always_false,
                           "normand" : normand,
                           }
        conf = { 
                "map": [ {"name": "route_1", 
                          "rules" : [{"name" : "user_is_local"} , {"name" : "job_name_match", 
                                                                   "parameters" : {"name": "Filochard"}
                                                                   }
                                     ],
                          "exec_system" : "big_one" 
                                      },
                         {"name" :"route_2",
                          "rules" : [{"name" : "project_match", "parameters" : {"name": "dans le cambouis"}} ],
                          "exec_system" : "small_one" 
                         },
                         {"name" : "default",
                          "rules" : [],
                          "exec_system" : "cluster_two" 
                          }
                        ]
               }
        self.push_routes_in_db(conf["map"])
        
        
    def tearDown(self):
        objects = connection.Job.find({})
        for obj in objects:
            obj.delete()
        objects = connection.User.find({})
        for obj in objects:
            obj.delete()
        objects = connection.Project.find({})
        for obj in objects:
            obj.delete()
        try:
            objects = connection.ExecutionRoutes.find({})
            for obj in objects:
                obj.delete()
        except AttributeError:
            pass
    
            
    def push_routes_in_db(self, conf_map):
        _map = connection.ExecutionRoutes()
        _map["map"] = conf_map
        _map.save()   
    
    def test_creation(self):
        rule = Rule("always_true")
        self.assertTrue(isinstance(rule, Rule))
        self.assertRaises(MobyleError, Rule, 'nimport na oik')
        
       
    def test_call(self):
        user = connection.User()
        user['email'] = 'foo@bar.fr'
        user.save()
        
        project = connection.Project()
        project['owner'] = user['_id']
        project['name'] = 'MyProject'
        project.save()
        
        status = Status(Status.INIT)
        
        job = connection.ProgramJob()
        job.project = project.id
        job.name = "first job"
        job.status = status
        job.owner = { 'toto': 'truc'}
        job.save()
        
        true = Rule("always_true")
        self.assertTrue(true(job) )
        false = Rule("always_false")
        self.assertFalse(false(job))
        normand_yes = Rule("normand", parameters = {'resp' : True})
        self.assertTrue(normand_yes(job))
        normand_no = Rule("normand", parameters = {'resp' : False})                
        self.assertFalse(normand_no(job))             
        