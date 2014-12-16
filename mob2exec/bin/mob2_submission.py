#! /usr/bin/env python
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
import glob
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
from mobyle.common.job import ProgramJob
from mobyle.common.job_routing_model import ExecutionSystem
from mobyle.common.job_routing_model import ExecutionRoutes, ExecutionRule

from mobyle.common.service import *
from mobyle.common.type import *


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

    
def program_generator():
    programs = []
    
    program_1 = connection.Program()
    program_1['name'] = 'golden'  
    program_1['command'] = 'golden '
    program_1['inputs'] = InputParagraph()

    bank = InputProgramParameter()
    bank['name'] = 'bank'
    bank['argpos'] = 10
    bank['mandatory'] = True
    bank['type'] = StringType()

    seq_id = InputProgramParameter()
    seq_id['name'] = 'seq_id'
    seq_id['argpos'] = 20
    seq_id['format'] = "' {bank}:{id}'.format(bank = bank, id = seq_id)"
    seq_id['type'] = StringType()

    program_1['inputs']['children'].append(bank)
    program_1['inputs']['children'].append(seq_id)
    
    program_1['outputs'] = OutputParagraph()
    stdout = OutputProgramParameter()
    stdout['name'] = 'stdout'
    stdout['output_type'] = 'stdout'
    sequence_type = FormattedType()
    sequence_type['format_terms'] = ['EDAM_format:0849']
    stdout['type'] = sequence_type
    stdout['filenames'] = "'golden.out'"
    program_1['outputs']['children'].append(stdout)
    
    stderr = OutputProgramParameter()
    stderr['name'] = 'stderr'
    stderr['output_type'] = 'stderr'
    sequence_type = FormattedType()
    sequence_type['format_terms'] = ['EDAM_format:1964']
    stderr['type'] = sequence_type
    stderr['filenames'] = "'golden.err'"
    program_1['outputs']['children'].append(stderr)
    
    program_1.save()
    
#     program_2 = connection.Program()
#     program_2['name'] = 'clutalw'  
#     program_2['command'] = 'clustalw2 -align '
#     program_2['inputs'] = InputParagraph()
# 
#     input_seq = InputProgramParameter()
#     input_seq['name'] = 'input_seq'
#     input_seq['argpos'] = 10
#     input_seq['mandatory'] = True
#     input_seq['type'] = StringType()
#     input_seq['format'] = '" -infile=" + str(value)'
    
    programs.append((program_1, {'bank':'uniprot_sprot', 'seq_id': '104k_thepa'}))
    programs.append((program_1, {'bank':'uniprot_sprot', 'seq_id': 'il2_human'}))
    programs.append((program_1, {'bank':'uniprot_sprot', 'seq_id': 'abcd3_rat'}))
    
    
    def get_program():
        return random.choice(programs)
    
    return get_program

get_program = program_generator()



def put_new_job_in_db(name, project):
    job = connection.ProgramJob()
    job.project = project.id 
    job.name = name
    job.status = Status(Status.TO_BE_BUILT)
    job.owner = {'id': project.id, 'klass': 'Project'}
    program, parameter_values = get_program()
    print "type program = {0}  parameter_values = {1}".format(type(program), parameter_values)
    job['service'] = program
    job['inputs'] = {}
    job.process_inputs(parameter_values)
    job.save()
 
    print("put new job {0} in db".format(job.id))
                    
def clean_dirs():
    projects_store = Config.config().get("DEFAULT","projects_store")
    projects_dirs = os.path.join(projects_store, 'projects')
    projects_dirs = glob.glob(os.path.join(projects_dirs, '*'))
    for p_dir in projects_dirs:
        if os.path.exists(p_dir):
            shutil.rmtree(p_dir)
    
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
    #project_directory = os.path.join(os.path.dirname(config.get("mob2exec","pid_file")), 
    #                                 'projects', 
    #                                 str(project.id))   
    #os.makedirs(project_directory, 0755) #create parent directory
    #project.dir = project_directory
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
                                 }],
            
                "map": [ {"name": "route_1", 
                          "rules" : [ExecutionRule({"name" : "user_is_local",
                                                   "parameters":None}), 
                                     ExecutionRule({"name" : "job_name_match", 
                                                    "parameters" : {"name": "Filochard"}
                                                                   })
                                     ],
                          "exec_system" : "big_one" 
                                      },
                         {"name" :"route_2",
                          "rules" : [ExecutionRule({"name" : "project_match", 
                                                    "parameters" : {"name": "dans le cambouis"}})],
                          "exec_system" : "small_one" 
                         },
                         {"name" : "default",
                          "rules" : [],
                          "exec_system" : "local" 
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
    
 #   for name in ('organisateurs de voyage', 'dans le cambouis', "l'Opération congélation"):
 #       projects[name] = create_project(random.choice(pieds_nickeles.values()), name)
    projects[name] = create_project(pieds_nickeles['Croquignol'], 'organisateurs de voyage')
                                        
    for i in range(0, 500):
        time.sleep(random.randint(0, 5))
        #j = cmdlines[ random.randint(1, 3)]
        put_new_job_in_db('golden', random.choice(projects.values()))
           
    print("no more job to add in DB")