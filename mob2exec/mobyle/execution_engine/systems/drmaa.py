# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from execution_system import ExecutionSystem

class DRMAA(ExecutionSystem):
    
    def __init__(self, name, drmaa_library_path = None, native_specifications = ""):
        super(DRMAA, self).__init__(name)
        self.drmaa_library_path = drmaa_library_path
        self.native_specifications = native_specifications
    
    def run(self):
        pass
    
    def get_status(self, job):
        pass
    
    def kill(self, job):
        pass
    
