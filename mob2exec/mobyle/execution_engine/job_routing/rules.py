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

from mobyle.common.mobyleError import MobyleError

_rules = {}

def register(func):
    """
    decorator which register a function as an available routing job rule
    ..note ::

      @rules.register
      def alacon(job):
           print job
    
    a rule must have only one argument named job
    """
    if func.func_name in _rules:
        raise MobyleError("there is several rules for routing job named \"{0}\"".format(func.func_name))
    argspec = inspect.getargspec(func)
    if len(argspec.args) != 1:
        raise MobyleError("invalid rules signature. rule need 1 argument, {0} ask {1:d}".format(func.func_name, len(argspec.args)))
    if argspec.args[0] != 'job':
        import warnings
        warnings.warn("rule take a 'job' as parameter, {0} defined with \"{1}\"".format(func.func_name, argspec.args[0]), SyntaxWarning)
    _rules[func.func_name] = func
    return func
    


def load_rules():
    """ 
    discover all modules in routing_rules package and load rules defined in them.
    :return: the register of rules
    :rtype: dict
    """
    global _rules
    if _rules:
        _rules= {}
    rules_path = os.path.abspath(os.path.join( os.path.dirname(__file__), '..','..', '..', 'routing_rules'))
    sys.path.insert(0, rules_path)
    for f in glob.glob(os.path.join(rules_path, '*.py')):
        module_name = os.path.splitext( os.path.basename(f))[0]
        if module_name != '__init__':
            __import__(module_name, globals(), locals(), [module_name])
    #clean the sys.path to avoid name collision
    sys.path.pop(0)
    return _rules
