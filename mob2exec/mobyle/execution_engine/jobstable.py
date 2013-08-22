# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import multiprocessing
import logging
_log = logging.getLogger(__name__)


class JobsTable(object):
    """
    maintain the list of active jobs. There is one jobsTable instance which is shared by the other processes 
     - the :class:`lib.execution_engine.db_manager.DBManager` instance, 
     - the :class:`lib.execution_engine.monitor.JtMonitor` instance, 
     - the :class:`lib.execution_engine.submit_actor.SubmitActor` instances, 
     - the :class:`lib.execution_engine.status_actor.StatusActor` instances, 
     - the :class:`lib.execution_engine.notification_actor.NotificationActor` instances
    """


    def __init__(self):
        """
        Constructor
        """
        self.manager = multiprocessing.Manager()
        self.jobs_table = self.manager.dict()
        self._lock = multiprocessing.Lock()
    
    def __iter__(self):
        jobs = self.jobs()    
        return iter( jobs )
    
    def put(self, job):
        """add a Job instance in table
        
        :param job: the Job instance to add in the table
        :type job: :class:`mobyle.common.job` instance
        """
        with self._lock:
            print "JobsTable.put job = " + str(job)
            print "JobsTable.put job.id = " + str(job.id)
            self.jobs_table[job.id] = job
    
    def get(self, job_id):
        """
        :param job_id: the id of a jobRef
        :type job_id: string
        :return: the job corresponding to the jobid without remove it from the table.
        :rtype: :class:`mobyle.common.job` instance
        
        """
        with self._lock:
            job = self.jobs_table[ job_id ]
        return job
    
    def jobs(self):
        """
        :return: the list of job contained in the table, sort by the increasing job.create_time
        :rtype: list of :class:`mobyle.common.job` instances
        
        """
        with self._lock:
            jobs = self.jobs_table.values()
            jobs.sort()
        return jobs
    
    def pop(self, job_id):
        """
        :param job_id: the id of a Job instance
        :type job_id: string
        :return: and remove the Job instance corresponding to job_id from the table.
        :rtype: :class:`mobyle.common.job` instance
        
        """
        with self._lock:
            job = self.jobs_table[job_id]
            del self.jobs_table[job_id]
        return job
    
    