#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup 
from distutils.dist import Distribution
from distutils.command.install_egg_info import install_egg_info
from distutils.command.build import build
from distutils.util import get_platform
from distutils.errors import DistutilsOptionError
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
                    ('build-lib=', None, "build directory for all modules (default: 'build.build-lib')"),
                    ('plat-name=', 'p', "platform name to build for, if supported (default: %s)" % get_platform()),
                    ]
    
    help_options = []
    

    def initialize_options(self):
        self.verbosity = None
        self.build_base = 'build'
        self.build_lib = None
        self.build_purelib = None
        self.build_platlib = None
        self.plat_name = None
        
        
    def finalize_options(self):
        #if self.build_lib is None:
        #    self.build_lib = os.path.join(self.build_base, 'lib' )
        if self.verbosity is None:
            self.verbosity = 0
        else:
            self.verbosity = int(self.verbosity)
            
        if self.plat_name is None:
            self.plat_name = get_platform()
        else:
            # plat-name only supported for windows (other platforms are
            # supported via ./configure flags, if at all).  Avoid misleading
            # other platforms.
            if os.name != 'nt':
                raise DistutilsOptionError(
                            "--plat-name only supported on Windows (try "
                            "using './configure --help' on your platform)")

        plat_specifier = ".%s-%s" % (self.plat_name, sys.version[0:3])

        # Make it so Python 2.x and Python 2.x with --with-pydebug don't
        # share the same build directories. Doing so confuses the build
        # process for C modules
        if hasattr(sys, 'gettotalrefcount'):
            plat_specifier += '-pydebug'

        # 'build_purelib' and 'build_platlib' just default to 'lib' and
        # 'lib.<plat>' under the base build directory.  We only use one of
        # them for a given distribution, though --
        if self.build_purelib is None:
            self.build_purelib = os.path.join(self.build_base, 'lib')
        if self.build_platlib is None:
            self.build_platlib = os.path.join(self.build_base,
                                              'lib' + plat_specifier)

        # 'build_lib' is the actual directory that we will use for this
        # particular module distribution -- if user didn't supply it, pick
        # one of 'build_purelib' or 'build_platlib'.
        if self.build_lib is None:
            if os.path.exists(self.build_purelib):
                self.build_lib = self.build_purelib
            elif os.path.exists(self.build_platlib):
                self.build_lib = self.build_platlib
            else:
                msg = """the builded lib cannot be found in {0} or {0}. 
 You must build the package before to test it (python setup.py build). 
 If you build it in other location, you must specify it for the test see options with "python setup.py test --help" """.format(self.build_purelib, self.build_platlib)
                raise DistutilsOptionError(msg)
                
    
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
            sys.exit("some tests fails. Run python setup.py test -vv to have more details")
        
            

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
                     'mobyle.execution_engine.engine',
                     'mobyle.execution_engine.job_routing',
                     'mobyle.execution_engine.systems',
                     'conf'
                     ],
      package_dir = {'': 'mob2exec'},
      scripts     = [ 'mob2exec/bin/mob2execd' ] ,
      cmdclass= { 'build' : check_and_build,
                  'test': test }
      )
