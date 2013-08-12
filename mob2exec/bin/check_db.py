#! /bin/env python
# -*- coding: utf-8 -*-

"""
Created on Aug 13, 2012

@author: Bertrand Néron
@contact: bneron@pasteur.fr
@organization: Institut Pasteur
@license: GPLv3
"""
import os 
import sys

MOBYLEHOME = None
if os.environ.has_key('MOBYLEHOME'):
    MOBYLEHOME = os.environ['MOBYLEHOME']
if not MOBYLEHOME:
    sys.exit('MOBYLEHOME must be defined in your environment')
 
if ( os.path.join( MOBYLEHOME, 'mob2exec' ) ) not in sys.path:
    sys.path.append(  os.path.join( MOBYLEHOME, 'mob2exec' )  )

from mobyle.common.config import Config
config = Config( os.path.abspath('../tests/test.conf'))
from mobyle.common.connection import connection

from mobyle.common.job import Status
from mobyle.common.job import ClJob


all_jobs = connection.ClJob.find({})
#all_jobs.sort()
for job in all_jobs:
    print "%s : %s : %s : %s : %s" % (job.id, job.name, job.status, job.create_time, job.end_time ) 