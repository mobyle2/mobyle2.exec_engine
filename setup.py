#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup 
from distutils.dist import Distribution
from distutils.command.install_egg_info import install_egg_info
from distutils.command.build import build
from distutils.core import Command
from distutils.versionpredicate import VersionPredicate
import time
import sys
import os

class check_and_build(build):
    def run(self):
        chk = True
        for req in require_pyt:
            chk &= self.chkpython(req)
        for req in require_mod:
            chk &= self.chkmodule(req)
        if not chk:
            sys.exit(1)
        build.run( self )

    def chkpython(self, req):
        chk = VersionPredicate(req)
        ver = '.'.join([str(v) for v in sys.version_info[:2]])
        if not chk.satisfied_by(ver):
            print >> sys.stderr, "Invalid python version, expected %s" % req
            return False
        return True

    def chkmodule(self, req):
        chk = VersionPredicate(req)
        try:
            mod = __import__(chk.name)
        except:
            print >> sys.stderr, "Missing mandatory %s python module" % chk.name
            return False
        for v in [ '__version__', 'version' ]:
            ver = getattr(mod, v, None)
            break
        try:
            if ver and not chk.satisfied_by(ver):
                print >> sys.stderr, "Invalid module version, expected %s" % req
                return False
        except:
            pass
        return True

class test(Command):
    
    description = "run the unit tests against the build library"
    
    user_options = [('verbosity' , 'v' , 'verbosity of outputs', 1),
                    ('build-base=', 'b', "base build directory (default: 'build.build-base')"),
                    ('build-lib=', None, "build directory for all modules (default: 'build.build-lib')")
                    ]
    
    help_options = []
    

    def initialize_options(self):
        self.verbosity = None
        self.build_base = 'build'
        self.build_lib = None
        
    def finalize_options(self):
        if self.build_lib is None:
            self.build_lib = os.path.join(self.build_base, 'lib' )
        if self.verbosity is None:
            self.verbosity = 0
        else:
            self.verbosity = int(self.verbosity)
    
    def run(self):
        """
        """
        sys.path.insert(0, os.path.join(os.getcwd(), 'tests'))
        import run_tests
        test_res = run_tests.run(self.build_lib, [], verbosity = self.verbosity)
        if not test_res.wasSuccessful():
            for error in test_res.errors:
                if error[0].__class__.__name__ == 'ModuleImportFailure':
                    err_cause = error[1].split('\n')[-3] #traceback ends with 2 \n
                    if err_cause.startswith('ImportError: No module named common.'):
                        sys.exit("""\nThe project mobyle2.lib is not installed or not in PYTHONPATH.
mobyle2.exec_engine depends of this project.                         
                        """)
        
            

class UsageDistribution(Distribution):
    
    def __init__(self,  attrs=None):
        Distribution.__init__(self, attrs = attrs)
        self.common_usage = """\
Common commands: (see '--help-commands' for more)

  setup.py build      will build the package underneath 'build/'
  setup.py test       will run the tests on the newly build library
  setup.py install    will install the package
"""



require_pyt = [ 'python (>=2.7, <3.0)' ]
require_mod = [ 'setproctitle (>=1.1.3)',
                'daemon (>=1.6)' ]


setup( distclass = UsageDistribution,
       name        = 'mobyle.exec_engine',
       version     =  time.strftime("%Y-%m-%d"),
       author      = "Néron Bertrand",
       author_email = "bneron@pasteur.fr" ,
       license      = "GPLv3" ,
       url = "https://github.com/mobyle2/mobyle2.exec_engine",
       download_url = "https://github.com/mobyle2/mobyle2.exec_engine",
       description  = "asynchron execution engine that allow Mobyle to submit jobs to DRMs",
       classifiers = [
                     'License :: GPLv3' ,
                     'Operating System :: POSIX' ,
                     'Programming Language :: Python' ,
                     'Topic :: Bioinformatics' ,
                    ] ,
      packages    = ['mobyle', 
                     'mobyle.execution_engine',
                     'conf'
                     ],
      package_dir = {'': 'mob2exec'},
      scripts     = [ 'mob2exec/bin/mob2execd' ] ,
      cmdclass= { 'build' : check_and_build,
                  'test': test }
      )
