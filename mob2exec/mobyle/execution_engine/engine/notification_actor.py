# -*- coding: utf-8 -*-

#===============================================================================
# Created on Aug 13, 2012
# 
# @author: Bertrand Néron
# @contact: bneron <at> pasteur <dot> fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import logging.config
import setproctitle

from .actor import Actor
        
class NotificationActor(Actor):
    """
    notify that the job corresponding to job_id is completed
    """
    
    def __init__(self, job_id, log_conf):
        """
        :param job_id: the id of the job to treat
        :type job_id: string
        """
        super(NotificationActor, self).__init__(job_id, log_conf)

           
    def run(self):
        self._name = "NotificationActor-{:d}".format(self.pid)
        setproctitle.setproctitle('mob2_notification')
        logging.config.dictConfig(self._log_conf)
        self._log = logging.getLogger(__name__) 
        
        job = self.get_job()
        if job.status.is_ended() and job.must_be_notified():
            job.has_been_notified = True
            self._log.debug( "{0} notified job {1} and put it in table".format(self._name, job.id))
            job.save()
        else:
            self._log.debug( "{0} job {1} must not be notified".format(self._name, job.id))
        self._log.debug( "{0} exiting".format(self._name))
        
        
