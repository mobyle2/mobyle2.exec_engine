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
from mobyle.common.job import Status, ProgramJob

from mobyle.common.mobyleError import MobyleError
from mobyle.execution_engine.job_routing.route import Rule
from mobyle.execution_engine.job_routing.route import Route
from mobyle.execution_engine.systems.local import Local


class RouteTest(unittest.TestCase):

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
        
        user = connection.User()
        user['email'] = 'foo@bar.fr'
        user.save()
        
        project = connection.Project()
        project['owner'] = user['_id']
        project['name'] = 'MyProject'
        project.save()
        
        status = Status(Status.INIT)
        
        self.job = connection.ProgramJob()
        self.job.project = project.id
        self.job.name = "first job"
        self.job.status = status
        self.job.owner = { 'toto': 'truc'}
        self.job.save()
        
        self.rule_true = Rule("always_true")
        self.rule_false = Rule("always_false")
        self.normand_yes = Rule("normand", parameters = {'resp' : True})
        self.normand_no = Rule("normand", parameters = {'resp' : False})                
        
        self.exec_sys = Local("execution local")
       
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
            
            
    def test_exec_sys(self):
        route = Route('66', self.exec_sys)
        self.assertEqual(route.exec_sys, self.exec_sys)
        
                          
    def test_append(self):
        route = Route('66', self.exec_sys)
        self.assertEqual(route.rules, [])
        route.append(self.rule_true)
        self.assertEqual(route.rules, [self.rule_true])
        
    def test_allow(self):
        route = Route('66', self.exec_sys, [self.rule_true])
        self.assertTrue(route.allow(self.job))
        route.append(self.rule_false)
        self.assertFalse(route.allow(self.job))
        