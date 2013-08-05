# -*- coding: utf-8 -*-

#==============================================
#Created on Aug 2, 2013
#
#@author: Bertrand Néron
#@contact: bneron@pasteur.fr
#@organization: Institut Pasteur
#@license: GPLv3
#==============================================

import os
import unittest
import time
from random import choice
import multiprocessing
import logging

#a config object must be instantiated first for each entry point of the application
from mobyle.common.config import Config
config = Config( os.path.join( os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection

from mobyle.execution_engine.jobstable import JobsTable
from mobyle.common.job import Status
from mobyle.common.job import ClJob
from mobyle.execution_engine.db_manager import DBManager

class DBManagerTest(unittest.TestCase):

    def setUp(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
       
    def tearDown(self):
        objects = connection.ClJob.find({})
        for obj in objects:
            obj.delete()
           
    def test_stop(self):
        jobs= []
        for i in range(0,10):
            job = connection.ClJob()
            job.name = "job_%d" % i
            job.owner = "me"
            job.status = Status(Status.RUNNING)
            job.save()
            jobs.append(job)
         
        for j in jobs:
            j.status.state = Status.FINISHED
              
        jt = JobsTable()
        db_q = multiprocessing.Queue()
        mgr = DBManager(jt, db_q)
        mgr._log = logging.getLogger()
           
        for j in jobs:
            jt.put(j)
          
        mgr.stop()
          
        jobs_from_DB = connection.ClJob.find({})
        for j in jobs_from_DB:    
            self.assertEqual(j.status, Status(Status.FINISHED))
              
             
    def test_update_jobs(self):
        jobs_to_update= []
        for i in range(0,10):
            job = connection.ClJob()
            job.name = "job_%d" % i
            job.owner = "me"
            job.status = Status(Status.RUNNING)
            job.save()
            jobs_to_update.append(job)
        
        job_to_keep = connection.ClJob()
        job_to_keep.name = "job_%d" % i
        job_to_keep.owner = "me"
        job_to_keep.status = Status(Status.RUNNING)
        job_to_keep.save()
                            
        for j in jobs_to_update:
            j.status.state = Status.FINISHED
            
        jt = JobsTable()
        db_q = multiprocessing.Queue()
        mgr = DBManager(jt, db_q)
        mgr._log = logging.getLogger()
        
        for j in jobs_to_update +[job_to_keep]:
            jt.put(j)
        
        mgr.update_jobs(jobs_to_update)
        
        jobs = connection.ClJob.find({})
        for j in jobs:
            if j.id == job_to_keep.id:
                self.assertEqual(j, job_to_keep)
            else:
                self.assertEqual(j.status, Status(Status.FINISHED))
                
            
            
    def test_get_new_jobs(self):
        jobs_send = []
        for i in range(0,10):
            job = connection.ClJob()
            job.name = "job_%d" % i
            job.owner = "me"
            if i%2 :
                job.status = Status(Status.TO_BE_SUBMITTED)
            else:
                job.status = Status(choice((Status.INIT, Status.BUILDING, Status.RUNNING, Status.FINISHED, Status.ERROR)))
            job.save()
            jobs_send.append(job)
            time.sleep(1)
        new_jobs_send = [j for j in jobs_send if j.status == Status.TO_BE_SUBMITTED]
        jt = JobsTable()
        db_q = multiprocessing.Queue()
        mgr = DBManager(jt, db_q)
        mgr._log = logging.getLogger()
        new_jobs = mgr.get_new_jobs()
        self.assertEqual(new_jobs_send, new_jobs)
        new_jobs_send.sort()
        new_jobs.sort()
        self.assertEqual(new_jobs_send, new_jobs)
        