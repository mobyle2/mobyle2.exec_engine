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

import shutil
import argparse
parser = argparse.ArgumentParser(description="check jobs in mobyle2 mongo DB")
parser.add_argument("-c", "--config",
                    action = 'store',
                    dest = 'cfg_file',
                    default = None,
                    help = "the path to a mobyle2 configuration file")
parser.add_argument( '--no-db',
                     action = 'store_true',
                     dest = 'no_db',
                     default = False,
                     help = "does not clean the DB"
                    )
parser.add_argument( '--no-dir',
                     action = 'store_true',
                     dest = 'no_dir',
                     default = False,
                     help = "does not clean projects directories"
                    )
                     
args = parser.parse_args()
config_path = os.path.abspath(args.cfg_file)
from mobyle.common.config import Config
config = Config(config_path).config()
from mobyle.common.connection import connection


from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import ProgramJob


def clean_db(connection):
    old_jobs = connection.Job.find({})
    for obj in old_jobs:
        obj.delete()
    
    old_users = connection.User.find({})
    for obj in old_users:
        obj.delete()
        
    old_projects = connection.Project.find({})
    for obj in old_projects:
        obj.delete()
        
        
def clean_dirs(config):
    projects_dir = os.path.join(os.path.dirname(config.get("mob2exec","pid_file")), 'projects')
    if os.path.exists(projects_dir):
        shutil.rmtree(projects_dir)

if __name__ == '__main__':

    
    if args.no_dir and args.no_db:
        sys.exit()
    elif args.no_dir:
        clean_db(connection)
    elif args.no_db:
        clean_dirs(config)  
    else:
        clean_db(connection)
        clean_dirs(config)
