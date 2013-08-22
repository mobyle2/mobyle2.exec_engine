# -*- coding: utf-8 -*-

#========================
# :Date:Aug 12, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================


import os 
import sys

MOBYLEHOME = None
if os.environ.has_key('MOBYLE_HOME'):
    MOBYLEHOME = os.environ['MOBYLE_HOME']
if not MOBYLEHOME:
    sys.exit('MOBYLE_HOME must be defined in your environment')

if (os.path.join(MOBYLEHOME , 'mob2exec')) not in sys.path:
    sys.path.insert(0, os.path.join(MOBYLEHOME , 'mob2exec'))

import time
import random
import argparse
parser = argparse.ArgumentParser(description="simulate submission from a user")
parser.add_argument("-c", "--config",
                    action = 'store',
                    dest = 'cfg_file',
                    default = None,
                    help = "the path to a mobyle2 configuration file")
args = parser.parse_args()

from mobyle.common.config import Config
config = Config( os.path.abspath(args.cfg_file)).config()
from mobyle.common.connection import connection

from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import ClJob


cmdlines={
        1:{ 'name':'blast', 
             'cmd_line': 'blastall -p blastp -d uniprot_sprot -i abcd2_mouse.fa -e 0.1 -o blast2.txt',
             'inputs':[( 'abcd2_mouse.fa', """>ABCD2_MOUSE RecName: Full=ATP-binding cassette sub-family D member 2; AltName: Full=Adrenoleukodystrophy-related protein;
MIHMLNAAAYRVKWTRSGAAKRAACLVAAAYALKTLYPIIGKRLKQPGHRKAKAEAYSPAENREILHCTEIICKKPAPGL
NAAFFKQLLELRKILFPKLVTTETGWLCLHSVALISRTFLSIYVAGLDGKIVKSIVEKKPRTFIIKLIKWLMIAIPATFV"""
              )]
             },
        2:{ 'name': 'golden', 
            'cmd_line': 'golden uniprot_sprot:abcd2_rat',
            'inputs':[]
            },
        3:{ 'name': 'clustalw',
            'cmd_line':'clustalw -align -infile=abcd.fasta',
            'inputs' : [( 'abcd.fasta' , """>1pgb
MTYKLILNGKTLKGEAVDAATAEKVFKQYANDNGVDGEWTYTKTFTVTE
>1txt
MTYKLILNGKTLKGETTTEAVDAATAEKVNDNGVDGEWTYDDATKTFTVTE
>1trt
MTYKLILNGKTLKGETTTEAVDAATAEKVFKQYANDNGVDGEWTYDDATKTFTVTE""")]
            }
        }    

pieds_nickeles = {}

projects = {}


def put_new_job_in_db(name, cmd_line, project ):
    job = connection.ClJob()
    job.project = project.id 
    job.name = name
    job.status = Status(Status.BUILDING)
    job.owner = {'id': project.id, 'klass': 'Project'}
    job.cmd_line = cmd_line
    job.save()

def clean_db():
    old_jobs = connection.Job.find({})
    for obj in old_jobs:
        obj.delete()
    
    old_users = connection.User.find({})
    for obj in old_users:
        obj.delete()
        
    old_projects = connection.Project.find({})
    for obj in old_projects:
        obj.delete()
        
def create_user(name):
    user = connection.User()
    user['email'] = '{0}@pieds.nickelés.fr'.format(name)
    user.save()
    return user

def create_project(user, name):
    project = connection.Project()
    project['owner'] = user['_id']
    project['name'] = name
    project.save()
    project_directory = os.path.join(os.path.dirname(config.get("mob2exec","pid_file")), 'projects', str(project.id))   
    os.makedirs(project_directory, 0755) #create parent directory
    return project

if __name__ == '__main__':
    
    clean_db()

    for name in ('Filochard', 'Ribouldingue', 'Croquignol'):
        pieds_nickeles[name] = create_user(name)
    
    for name in ('organisateurs de voyage' , 'dans le cambouis' , "l'Opération congélation"):
        projects[name] = create_project(random.choice(pieds_nickeles.values()), name)
    
    for i in range(0, 500):
        time.sleep(random.randint(0, 5))
        j = cmdlines[ random.randint(1,3)]
        print( "put new job %s in db" % j['name'])
        put_new_job_in_db( j['name'], j['cmd_line'], random.choice(projects.values()))
           
    print( "no more job to add in DB")