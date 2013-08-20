#! /usr/bin/env python
# -*- coding: utf-8 -*-

#===============================================================================
# Created on Mar 07, 2013
# 
# @author: Bertrand Néron
# @contact: bneron@pasteur.fr
# @organization: Institut Pasteur
# @license: GPLv3
#===============================================================================

import os
import sys
import unittest


if not os.environ['MOBYLE_HOME']:
    MOBYLE_HOME = os.path.abspath( os.path.join( os.path.dirname( __file__ ), "../", "mob2exec"))
    os.environ['MOBYLE_HOME'] = MOBYLE_HOME
MOBYLE_HOME = os.environ['MOBYLE_HOME']
if (MOBYLE_HOME) not in sys.path:
    sys.path.append(MOBYLE_HOME)
  


def run(tests, verbosity = 0):

    if not tests:
        suite = unittest.TestLoader().discover(os.path.dirname(__file__), pattern="test_*.py" ) 
    else:
        suite = unittest.TestSuite()
        for test in args.tests: 
            if os.path.exists(test):
                if os.path.isfile(test):
                    fpath, fname =  os.path.split( test )
                    suite.addTests(unittest.TestLoader().discover(fpath , pattern = fname )) 
                elif os.path.isdir(test):  
                    suite.addTests(unittest.TestLoader().discover(test)) 
            else:
                sys.stderr.write(  "%s : no such file or directory\n" % test) 

    unittest.TextTestRunner(verbosity = verbosity).run(suite)


if __name__ == '__main__':

    from argparse import ArgumentParser    
    parser = ArgumentParser()
    parser.add_argument("tests",
                        nargs = '*',
                        default = [],
                        help = "name of test to execute")
    
    parser.add_argument("-v", "--verbose" , 
                        dest= "verbosity" , 
                        action="count" , 
                        help= "set the verbosity level of output",
                        default = 0
                        )
    
    args = parser.parse_args()
    run(args.tests , args.verbosity)
