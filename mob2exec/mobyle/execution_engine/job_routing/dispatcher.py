# -*- coding: utf-8 -*-

#========================
# :Date:Aug 26, 2013
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: GPLv3
#========================


from .rules import load_rules


class Dispatcher(object):
    
    def __init__(self):
        self.rules = load_rules()
