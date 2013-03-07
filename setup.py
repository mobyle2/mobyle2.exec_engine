#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup
from distutils.command.install_egg_info import install_egg_info
from distutils.command.build import build
from distutils.versionpredicate import VersionPredicate
import time
import sys

class check_and_build( build ):
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


    
require_pyt = [ 'python (>=2.7, <3.0)' ]
require_mod = [ 'setproctitle (>=1.1.3)',
                'daemon (>=1.6)' ]


setup(name        = 'mobyle.exec_engine',
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
      cmdclass= { 'build' : check_and_build }
      )
