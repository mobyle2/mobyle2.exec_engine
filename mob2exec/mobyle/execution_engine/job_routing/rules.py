# -*- coding: utf-8 -*-

#========================
# :Date:Aug 26, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================

import glob
import os
import sys
import inspect

#from mongokit import CustomType

from mobyle.common.error import InternalError




def make_register():
    """
    generator of register. capture rules in closure accessible from register and load_rules
    """
    rules = {}
    module_imported = {}
    
    def register(func):
        """
        decorator which register a function as an available routing job rule
        ..note ::
    
          @rules.register
          def alacon(job):
               print job
        
        a rule must have only one argument named job
        """
        if func.func_name in rules:
            raise InternalError("there is several rules for routing job named \"{0}\"".format(func.func_name))
        argspec = inspect.getargspec(func)
        if not len(argspec.args) >= 1:
            raise InternalError("invalid rules signature. rule need at least 1 a job as first argument".format(func.func_name))
        if argspec.args[0] != 'job':
            import warnings
            warnings.warn("rule take a 'job' as parameter, {0} defined with \"{1}\"".format(func.func_name, argspec.args[0]), SyntaxWarning)
        rules[func.func_name] = func
        return func

    def load_rules():
        """ 
        discover all modules in routing_rules package and load rules defined in them.
        
        if load_rules is called twice, new modules are taken in account, but the
        deleted module or modification of module are ignored.
        
        :return: the register of rules
        :rtype: dict
        """
        def load(path):
            sys.path.insert(0, path)
            for f in glob.glob(os.path.join(path, '*.py')):
                module_name = os.path.splitext( os.path.basename(f))[0]
                if module_name != '__init__':
                    if module_name in module_imported:
                        pass
                    else:
                        module = __import__(module_name, globals(), locals(), [module_name])
                        module_imported[module_name] = module
            #clean the sys.path to avoid name collision
            sys.path.pop(0)
            
        mobyle_rules_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "routing_rules"))
        load(mobyle_rules_path)
        user_rules_path = os.path.abspath(os.path.join( os.path.dirname(__file__), '..', '..', '..', 'routing_rules'))
        load(user_rules_path)
        return rules.copy()
    return register, load_rules
    

register, load_rules = make_register()
    