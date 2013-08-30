# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

import os

from drmaa import DRMAA
from execution_system import register

@register
class OgsDRMAA(DRMAA):
    
    def __init__(self, name, drmaa_library_path = None, root = None, cell = None,  native_specifications = ""):
        super(OgsDRMAA, self).__init__(name, drmaa_library_path, native_specifications = native_specifications)
        os.environ['SGE_ROOT'] = root
        os.environ['SGE_CELL'] = cell