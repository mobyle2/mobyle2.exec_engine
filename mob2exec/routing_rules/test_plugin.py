# -*- coding: utf-8 -*-

#========================
# :Date:Aug 26, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from mobyle.execution_engine.job_routing import rules

@rules.register
def alacon(job):
    print job

@rules.register
def alacon_bis(zette):
    print zette
    
def alacon_ter(job):
    print job

@rules.register
def user_is_local(job):
    p = connection.Project.find_one({'_id': job.project})
    return job.name == 'Filochard'   
    
@rules.register
def job_name_match(job, name = None):
    return job.name == name


from mobyle.common.connection import connection
from mobyle.common.project import Project

@rules.register
def project_match(job, name = None):
    
    project = job.get_project()
    return project['name'] == name
