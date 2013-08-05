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
from mobyle.execution_engine.jobstable import JobsTable
from mobyle.common.job import Status
from mobyle.common.job import ClJob

class JobsTableTest(unittest.TestCase):

    def setUp(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
       
    def tearDown(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
           
    def test_put_get_pop(self):
        jt = JobsTable()
        j1 = connection.ClJob()
        j1.name = "first job"
        j1.status = Status(Status.BUILDING)
        j1.owner = "me"
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
            j.name = "job_%d" % i
            j.status = Status(Status.BUILDING)
            j.owner = "me"
            j.save()
            jobs_send.append(j)
            #the sensibilty of creation is 1sec minimum
            #so to have a predictable order between job in the list I need to wait for 1 sec 
            time.sleep(1)
        for j in jobs_send:
            jt.put(j)
        jobs_recieved = jt.jobs()  
        self.assertEqual(jobs_send, jobs_recieved)
        for j in  jobs_recieved:
            j.save()
        
