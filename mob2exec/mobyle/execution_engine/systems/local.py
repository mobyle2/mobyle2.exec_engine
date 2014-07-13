# -*- coding: utf-8 -*-

#========================
# :Date:Aug 28, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

from execution_system import ExecutionSystem, register

@register
class Local(ExecutionSystem):
    
    def __init__(self, name, native_specifications = ""):
        super(Local, self).__init__(name)
        self.native_specifications = native_specifications
    
    def run(self):
        pass
    
    def get_status(self, job):
        pass
    
    def kill(self, job):
        pass