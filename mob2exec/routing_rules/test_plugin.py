# -*- coding: utf-8 -*-

#========================
# :Date:Aug 26, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from mobyle.execution_engine.job_routing import rules

@rules.register
def alacon(job):
    print job

@rules.register
def alacon_bis(zette):
    print zette
    
def alacon(job):
    print job

