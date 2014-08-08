# -*- coding: utf-8 -*-

#========================
# :Date:Aug 29, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================


import sys
sys.path.insert(0, '/home/bneron/Mobyle/2.0/mobyle2.exec_engine/mob2exec')

import random
from itertools import izip

from mobyle.common.config import Config
config = Config('/home/bneron/Mobyle/2.0/mobyle2.conf/mobyle.ini')
from mobyle.common.connection import connection
from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import ClJob
from mobyle.common.job_routing_model import ExecutionSystem
from mobyle.common.job_routing_model import ExecutionRoutes

from mobyle.common.mobyleError import MobyleError  

    

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
    return project

def create_job(name, project ):
    job = connection.ClJob()
    job.project = project.id 
    job.name = name
    job.status = Status(Status.TO_BE_BUILT)
    job.owner = {'id': project.id, 'klass': 'Project'}
    job.save()
    return job

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


################### Preparation ##########################

clean_db()

pieds_nickeles = {}
projects = []
name_str = ('Filochard', 'Ribouldingue', 'Croquignol')
p_str = ('organisateurs de voyage', 'dans le cambouis', "l'Opération congélation")
for u_name ,p_name  in izip(name_str, p_str):
    pieds_nickeles[u_name] = create_user(u_name)
    projects.append(create_project(pieds_nickeles[u_name], p_name))
      
      
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

############## Test #####################################
#from mobyle.execution_engine.systems.execution_system import load_execution_classes
#from mobyle.execution_engine.job_routing.route import Rule, Route, Dispatcher
#from mobyle.execution_engine.job_routing.route import Dispatcher
from mobyle.execution_engine.job_routing.route import get_dispatcher

recieved_routes = []    
dispatcher = get_dispatcher()

for i in range(0, 3):
    job = create_job(name_str[i], projects[i])
    print "----------------"
    print "job name ", name_str[i]
    print "project ", p_str[i]
    recieved_routes.append((job, dispatcher.which_route(job)))

print "======================="

for item in recieved_routes:
    print "----------------"
    print "job name", item[0].name
    print "route name", item[1].name
    
