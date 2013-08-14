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
if os.environ.has_key('MOBYLEHOME'):
    MOBYLEHOME = os.environ['MOBYLEHOME']
if not MOBYLEHOME:
    sys.exit('MOBYLEHOME must be defined in your environment')

if ( os.path.join( MOBYLEHOME , 'mob2exec' ) ) not in sys.path:
    sys.path.append( os.path.join( MOBYLEHOME , 'mob2exec' )  )

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
config = Config( os.path.abspath(args.cfg_file))
from mobyle.common.connection import connection

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
    
    
def put_new_job_in_db(name, cmd_line):
    job = connection.ClJob()
    job.name = name
    job.status = Status(Status.BUILDING)
    job.owner = "me"
    job.cmd_line = cmd_line
    job.save()

def clean_db():
    old_jobs = connection.Job.find({})
    for obj in old_jobs:
        obj.delete()

clean_db()
for i in range(0, 500):
    time.sleep(random.randint(0, 5))
    j = cmdlines[ random.randint(1,3)]
    print( "put new job %s in db" % j['name'])
    put_new_job_in_db( j['name'], j['cmd_line'])
           
print( "no more job to add in DB")