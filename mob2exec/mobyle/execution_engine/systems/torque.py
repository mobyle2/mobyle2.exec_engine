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
class TorqueDRMAA(DRMAA):
    
    def __init__(self, name, drmaa_library_path = None, server_name = None, native_specifications = ""):
        super(TorqueDRMAA, self).__init__(drmaa_library_path, native_specifications = native_specifications)
        self.contactString = server_name
