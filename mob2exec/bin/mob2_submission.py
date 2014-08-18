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
import shutil
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
from mobyle.common.job_routing_model import ExecutionSystem
from mobyle.common.job_routing_model import ExecutionRoutes

cmdlines = {
        1 : { 'name':'blast', 
             'cmd_line': 'blastall -p blastp -d uniprot_sprot -i abcd2_mouse.fa -e 0.1 -o blast2.txt',
             'inputs':[( 'abcd2_mouse.fa', """>ABCD2_MOUSE RecName: Full=ATP-binding cassette sub-family D member 2; AltName: Full=Adrenoleukodystrophy-related protein;
MIHMLNAAAYRVKWTRSGAAKRAACLVAAAYALKTLYPIIGKRLKQPGHRKAKAEAYSPAENREILHCTEIICKKPAPGL
NAAFFKQLLELRKILFPKLVTTETGWLCLHSVALISRTFLSIYVAGLDGKIVKSIVEKKPRTFIIKLIKWLMIAIPATFV"""
              )]
             },
        2 : { 'name': 'golden', 
            'cmd_line': 'golden uniprot_sprot:abcd2_rat',
            'inputs':[]
            },
        3 : { 'name': 'clustalw',
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
        
    try:
        old_exec_sys = connection.ExecutionSystem.find({})
        for obj in old_exec_sys:
            obj.delete()
    except AttributeError:
        print >> sys.stderr, "collection ExecutionSystem not found"
    try:
        old_routes = connection.ExecutionRoutes.find({})
        for obj in old_routes:
            obj.delete()
    except AttributeError:
        print >> sys.stderr, "collection ExecutionRoutes not found"

        
def push_exec_sys_in_db(conf):
    exec_sys = connection.ExecutionSystem()
    exec_sys['_id'] = conf['_id']
    exec_sys['class'] = conf['class']
    if "drm_options" in conf:
        exec_sys["drm_options"] = conf["drm_options"]
    if "native_specifications" in conf:
        exec_sys["native_specifications"] = conf["native_specifications"]
    exec_sys.save()
    
        
def push_routes_in_db(conf_map):
    _map = connection.ExecutionRoutes()
    _map["map"] = conf_map
    _map.save()   

    
def put_new_job_in_db(name, cmd_line, project ):
    job = connection.ClJob()
    job.project = project.id 
    job.name = name
    job.status = Status(Status.TO_BE_BUILT)
    job.owner = {'id': project.id, 'klass': 'Project'}
    job.cmd_line = cmd_line
    job.save()
    print("put new job {0} in db".format(job.id))
                    
def clean_dirs():
    projects_dir = os.path.join(os.path.dirname(config.get("mob2exec","pid_file")), 'projects')
    if os.path.exists(projects_dir):
        shutil.rmtree(projects_dir)
    
def create_user(name):
    user = connection.User()
    user['email'] = '{0}@pieds.nickelés.fr'.format(name)
    user['first_name'] = name
    user.save()
    return user

def create_project(user, name):
    project = connection.Project()
    project['owner'] = user['_id']
    project['name'] = name
    project.save()
    project_directory = os.path.join(os.path.dirname(config.get("mob2exec","pid_file")), 
                                     'projects', 
                                     str(project.id))   
    os.makedirs(project_directory, 0755) #create parent directory
    project.dir = project_directory
    project.save()
    return project


if __name__ == '__main__':
    
    clean_db()
    clean_dirs()
    
    ######################################
    # push conf for job routing in mongodb
    ######################################
    conf = { "execution_systems" : [{"_id" : "big_one",
                              "class" : "OgsDRMAA",
                              "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                               "cell" : '/usr/local/sge',
                                               "root" : 'default', 
                                               },
                                "native_specifications": " -q mobyle-long " 
                                },
                                {"_id" : "small_one",
                                 "class" : "OgsDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                                  "cell" : '/usr/local/sge',
                                                  "root" : 'default' 
                                                  },
                                 "native_specifications": " -q mobyle-small " 
                                 },
                                {"_id" : "cluster_two",
                                 "class" : "TorqueDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/torque/libdrmaa.so",
                                                  "server_name" : "localhost" 
                                                  },
                                 "native_specifications": " -q mobyle-small " 
                                 },
                                {"_id" : "local",
                                 "class" : "Local",
                                 "native_specifications" : " nice -n 18 "
                                 }],
            
                "map": [ {"name": "route_1", 
                          "rules" : [{"name" : "user_is_local"} , {"name" : "job_name_match", 
                                                                   "parameters" : {"name": "Filochard"}
                                                                   }
                                     ],
                          "exec_system" : "big_one" 
                                      },
                         {"name" :"route_2",
                          "rules" : [{"name" : "project_match", "parameters" : {"name": "dans le cambouis"}} ],
                          "exec_system" : "small_one" 
                         },
                         {"name" : "default",
                          "rules" : [],
                          "exec_system" : "cluster_two" 
                          }
                        ]
               }
    
    for exec_sys in conf["execution_systems"]:
        push_exec_sys_in_db(exec_sys)

    push_routes_in_db(conf["map"])  
     
     
    #####################################################
    # create Users, Projects, Job and push job in mongodb 
    #####################################################
            
    for name in ('Filochard', 'Ribouldingue', 'Croquignol'):
        pieds_nickeles[name] = create_user(name)
    
    for name in ('organisateurs de voyage', 'dans le cambouis', "l'Opération congélation"):
        projects[name] = create_project(random.choice(pieds_nickeles.values()), name)
    
    for i in range(0, 500):
        time.sleep(random.randint(0, 5))
        j = cmdlines[ random.randint(1, 3)]
        put_new_job_in_db(j['name'], j['cmd_line'], random.choice(projects.values()))
           
    print("no more job to add in DB")