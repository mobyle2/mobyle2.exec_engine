# -*- coding: utf-8 -*-

#===============================================================================
#   Created on Aug 13, 2012           
#                                     
# @author: Bertrand Néron              
# @contact: bneron@pasteur.fr          
# @organization: Institut Pasteur      
# @license: GPLv3                      
#===============================================================================

import logging
_log = logging.getLogger(__name__)

import abc

class AbstractJobRef(object):
    """
    AbstractJobRef is an abstract class that describes the common interface of JobRef and WorkflowRef
    """     
    __metaclass__ = abc.ABCMeta

    def __init__(self, id , create_time, status, owner ):
        """

        :param id: the identifier of this jobRef
        :type id: string
        :param create_time: the time of job creation
        :type create_time: time.struct_time
        :param status: the status of this jobRef
        :type status:  :class:`lib.core.status.Status` object.
        :param owner: owner id of the job: either a workflow (local|remote) or a userspace (local|remote)
        :type owner: string
        
        """
        self._id = id
        self._owner = owner
        self._create_time = create_time
        self.status = status
        
    def __cmp__(self, other):
        """
        :param other: a JobRef I want to comared with self
        :type other: an :class:`lib.core.jobref.AbstractJobRef` object.
        :returns: negative int if the other object was created before this one, positive integer if other is newer, or 0 if they are created at the same time.
        :rtype: Integer 
        
        """
        return cmp( self.create_time , other.create_time )
    
    @property
    def id(self):
        """
        :returns: the unique identifier of this job.
        :rtype: string
        """
        return self._id

    @property
    def owner(self):
        """
        :return: the owner of a job. It can be a user space or a workflow
        :rtype: ???
         
        """
        return self._owner
    
    @property
    def create_time(self):
        """
        :returns: the time of job creation
        :rtype: time from epoch??, time struct ??
        
        """
        return self._create_time

    @abc.abstractmethod
    def must_be_notified(self):
        """
        :returns: True if a notification must be send a the end of job. False otherwise 
        
        """
    
class JobRef(AbstractJobRef):
    """
    AbstractJobRef implementation for a simple job
    """ 
    
    def __init__(self, id , create_time, status, owner ):
        """
        :param id: the identifier of this jobRef
        :type id: string
        :param create_time: the time of job creation
        :type create_time: time.struct_time
        :param status: the status of this jobRef
        :type status: :class:`lib.core.status.Status` object.
        :param owner: "owner" id of the job: either a workflow( local or remote) or a userspace(local or remote)
        :type owner: ???
        
        """
        super(JobRef , self).__init__( id, create_time, status, owner )
        self.end_time = -1
        self.has_been_notified = False

    def must_be_notified(self):
        """
        :returns: True if a notification must be send a the end of job. False otherwise 
        
        """
        delay = 60
        return True if self.end_time - self.create_time > delay else False 
        
        


