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
from mobyle.common.config import Config
config = Config( os.path.join( os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection

from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status, ProgramJob, Job

from mobyle.common.mobyleError import MobyleError
from mobyle.execution_engine.job_routing.route import Rule
from mobyle.execution_engine.job_routing.route import Route
from mobyle.execution_engine.job_routing.route import Dispatcher
from mobyle.execution_engine.systems.local import Local


class DispatcherTest(unittest.TestCase):

    def setUp(self):
        
        def job_name_match(job, name = None):
            return job.name == name
        
        def is_local(job):
            return job.owner['name'] == 'bidule'
            
        Rule.rules_reg = { "is_local" : is_local,
                           "job_name_match" : job_name_match,
                           }
        
        user = connection.User()
        user['email'] = 'foo@bar.fr'
        user.save()
        
        project = connection.Project()
        project['owner'] = user['_id']
        project['name'] = 'MyProject'
        project.save()
        
        status = Status(Status.INIT)
        
        self.job_1 = connection.ProgramJob()
        self.job_1.project = project.id
        self.job_1.name = "first job"
        self.job_1.status = status
        self.job_1.owner = {'name': 'truc'}
        self.job_1.save()
        
        self.job_2 = connection.ProgramJob()
        self.job_2.project = project.id
        self.job_2.name = "2nd job"
        self.job_2.status = status
        self.job_2.owner = {'name': 'bidule'}
        self.job_2.save()
        
        self.job_3 = connection.ProgramJob()
        self.job_3.project = project.id
        self.job_3.name = "first job"
        self.job_3.status = status
        self.job_3.owner = {'name': 'bidule'}
        self.job_3.save()
        
        self.job_4 = connection.ProgramJob()
        self.job_4.project = project.id
        self.job_4.name = "foo"
        self.job_4.status = status
        self.job_4.owner = {'name': 'bar'}
        self.job_4.save()
        
        self.rule_job_first = Rule("job_name_match", parameters = {'name' : 'first job'})
        self.rule_is_local = Rule("is_local")
        self.exec_sys = Local("execution local")
       
    def test_append(self):
        d = Dispatcher()
        route = Route('66', self.exec_sys)
        d.append(route)
        self.assertEqual(len(d), 1)
        self.assertEqual(d.popitem(0), ('66', route))
        
    def test_wich_test(self):
        d = Dispatcher()
        default = Route('default', self.exec_sys)
        route_1 = Route('route_1A', self.exec_sys, [self.rule_job_first])
        route_2 = Route('route_2A', self.exec_sys, [self.rule_is_local])
        route_3 = Route('route_3A', self.exec_sys, [self.rule_job_first, self.rule_is_local])
        d.append(route_3)
        d.append(route_2)  
        d.append(route_1) 
        d.append(default)
         
        #route_1 rule_job_first = job name = first job
        self.assertEqual(d.which_route(self.job_1), route_1)  
        #route_2 rule_is_local owner = bidule
        self.assertEqual(d.which_route(self.job_2), route_2)
        #route_3 rule_job_first = job name = first job , rule_is_local owner = bidule 
        self.assertEqual(d.which_route(self.job_3), route_3)
        #default
        self.assertEqual(d.which_route(self.job_4), default)