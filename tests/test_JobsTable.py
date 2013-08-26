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
import time

#a config object must be instantiated first for each entry point of the application
from mobyle.common.config import Config
config = Config( os.path.join( os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection

from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import ClJob

from mobyle.execution_engine.engine.jobstable import JobsTable


class JobsTableTest(unittest.TestCase):

    def setUp(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
            
        self.user = connection.User()
        self.user['email'] = 'foo@bar.fr'
        self.user.save()
        
        self.project = connection.Project()
        self.project['owner'] = self.user['_id']
        self.project['name'] = 'MyProject'
        self.project.save()
        
       
    def tearDown(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
        objects = connection.User.find({})
        for obj in objects:
            obj.delete()
        objects = connection.Project.find({})
        for obj in objects:
            obj.delete()
            
                       
    def test_put_get_pop(self):
        jt = JobsTable()
        j1 = connection.ClJob()
        j1.project = self.project.id
        j1.name = "first job"
        j1.status = Status(Status.BUILDING)
        j1.owner = {'id': self.project.id, 'klass': 'Project'}
        j1.save()
        jt.put(j1)
        self.assertEqual(len(jt.jobs()), 1)
        j2 = jt.get(j1.id)
        self.assertEqual(j1, j2)
        self.assertEqual(len(jt.jobs()), 1)
        j3 = jt.pop(j1.id)
        self.assertEqual(j1, j3)
        self.assertEqual(len(jt.jobs()), 0)
         
         
    def test_jobs(self):
        jt = JobsTable()
        jobs_send = []
        for i in range(0, 5):
            j = connection.ClJob()
            j.project = self.project.id
            j.name = "job_%d" % i
            j.status = Status(Status.BUILDING)
            j.owner = {'id': self.project.id, 'klass': 'Project'}
            j.save()
            jobs_send.append(j)
            #the sensibilty of creation is 1sec minimum
            #so to have a predictable order between job in the list I need to wait for 1 sec 
            time.sleep(1)
        for j in jobs_send:
            jt.put(j)
        jobs_recieved = jt.jobs()  
        self.assertEqual(jobs_send, jobs_recieved)
        
