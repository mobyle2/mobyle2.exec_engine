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
if os.environ.has_key('MOBYLE_HOME'):
    MOBYLEHOME = os.environ['MOBYLE_HOME']
if not MOBYLEHOME:
    sys.exit('MOBYLE_HOME must be defined in your environment')
 
if (os.path.join( MOBYLEHOME, 'mob2exec')) not in sys.path:
    sys.path.insert(0, os.path.join( MOBYLEHOME, 'mob2exec'))

import argparse
parser = argparse.ArgumentParser(description="check jobs in mobyle2 mongo DB")
parser.add_argument("-c", "--config",
                    action = 'store',
                    dest = 'cfg_file',
                    default = None,
                    help = "the path to a mobyle2 configuration file")
args = parser.parse_args()

from mobyle.common.config import Config
config = Config( os.path.abspath(args.cfg_file))
from mobyle.common.connection import connection

from mobyle.common.job import Status
from mobyle.common.job import ProgramJob


all_jobs = connection.Job.find({})
#all_jobs.sort()
for job in all_jobs:
    print "{0} : {1} : {2} : {3} : {4}".format(job.id, job.name, job.status, job.create_time, job.end_time) 