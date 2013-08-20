#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup 
from distutils.dist import Distribution
from distutils.command.install_egg_info import install_egg_info
from distutils.command.build import build
from distutils.versionpredicate import VersionPredicate
import time
import sys

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

class test_mobexec_engine(build):
    
    description = "run the unit tests against the build library"
    
    user_options = [ ('verbosity' , 'v' , 'verbosity of outputs', 1)]
    
    help_options = []
    

    def initialize_options(self):
        build.initialize_options(self)
        self.verbosity = None
        
    def finalize_options(self):
        if self.verbosity is None:
            self.verbosity = 0
        else:
            self.verbosity = int(self.verbosity)
    
    
    def run(self):
        """
        """
        import os
        this_dir = os.getcwd()

        # change to the test dir and run the tests
        os.chdir("tests")
        sys.path.insert(0, '')
        import run_tests
        try:
            import mobyle.common
        except ImportError:
            sys.exit("""The project mobyle2.lib is not installed or not in PYTHONPATH.
skip tests.""")
        run_tests.run(self.build_lib, [], verbosity = self.verbosity)

        # change back to the current directory
        os.chdir(this_dir)



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
      packages    = ['mobyle' , 'mobyle.execution_engine'],
      package_dir = {'': 'mob2exec'},
      scripts     = [ 'mob2exec/bin/mob2execd' ] ,
      cmdclass= { 'build' : check_and_build,
                  'test': test_mobexec_engine }
      )
