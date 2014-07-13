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
config = Config('/home/bneron/Mobyle/2.0/mobyle2.web/development.ini')

from mobyle.common.connection import connection
from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import ClJob

from mobyle.common.mobyleError import MobyleError  
from mobyle.execution_engine.systems.execution_system import load_execution_classes
from mobyle.execution_engine.job_routing.route import Rule, Route, Dispatcher
    
    
conf = { "execution_systems" : [{"name" : "big_one",
                              "class" : "OgsDRMAA",
                              "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                               "cell" : '/usr/local/sge',
                                               "root" : 'default', 
                                               },
                                "native_specifications": " -q mobyle-long " 
                                },
                                {"name" : "small_one",
                                 "class" : "OgsDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/sge/libdrmaa.so",
                                                  "cell" : '/usr/local/sge',
                                                  "root" : 'default' 
                                                  },
                                 "native-options": " -q mobyle-small " 
                                 },
                                {"name" : "cluster_two",
                                 "class" : "TorqueDRMAA", 
                                 "drm_options" : {"drmaa_library_path" : "path/to/torque/libdrmaa.so",
                                                  "server_name" : "localhost" 
                                                  },
                                 "native_specifications": " -q mobyle-small " 
                                 },
                                {"name" : "local",
                                 "class" : "Local",
                                 "native_specifications" : " nice -n 18 "
                                 }],
            
                "map": [ ("route_1", {"rules" : [{"name" : "user_is_local"} , {"name" : "job_name_match", 
                                                                              "parameters" : {"name": "Filochard"}}],
                                      "exec_sys" : "big_one" 
                                      }),
                         ("route_2", {"rules" : [{"name" : "project_match",
                                                  "parameters" : {"name": "dans le cambouis"}} ],
                                      "exec_sys" : "small_one" 
                                      }),
                         ("default", {"rules" : [],
                                      "exec_sys" : "cluster_two" 
                                      })
                        ]
               } 
            
exec_klass = load_execution_classes()
exec_systems = {}
for exec_conf in conf["execution_systems"]:
    try:
        klass = exec_klass[exec_conf["class"]]
    except KeyError, err:
        raise MobyleError('class {0} does not exist check your config'.format(exec_conf["class"]))
    opts = exec_conf["drm_options"] if "drm_options" in exec_conf else {}
    opts.update({"native_specifications" : exec_conf["native_specifications"]} if "native_specifications" in exec_conf else {})
    try:
        exec_systems[exec_conf["name"]] = klass( exec_conf["name"], **opts )
    except Exception, err:
        print exec_conf["name"]
        print opts
        print err

dispatcher = Dispatcher()

for route_conf in conf["map"]:
    rules = []
    for rule_conf in route_conf[1]["rules"]:
        rule = Rule(rule_conf["name"], parameters = rule_conf["parameters"] if "parameters" in rule_conf else {})
        rules.append(rule)
    exec_sys = exec_systems[route_conf[1]["exec_sys"]]
    route = Route(route_conf[0], exec_systems, rules )
    dispatcher.append(route)


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
    return project

def create_job(name, project ):
    job = connection.ClJob()
    job.project = project.id 
    job.name = name
    job.status = Status(Status.TO_BE_BUILT)
    job.owner = {'id': project.id, 'klass': 'Project'}
    job.save()
    return job

clean_db()

pieds_nickeles = {}
projects = []
name_str = ('Filochard', 'Ribouldingue', 'Croquignol')
p_str = ('organisateurs de voyage', 'dans le cambouis', "l'Opération congélation")
for u_name ,p_name  in izip(name_str, p_str):
    pieds_nickeles[u_name] = create_user(u_name)
    projects.append(create_project(pieds_nickeles[u_name], p_name))
              

recieved_routes = []    

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
    
