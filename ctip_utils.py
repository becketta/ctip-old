#
# Created by Aaron Beckett January, 2016
#

import math
from string import Template

###########################################################
#   Utility Classes
#

class CTIPError(Exception):
    """Base class for ctip exceptions."""
    def __init__(self, msg):
        self.msg = msg

class QsubBuilder(Template):
    delimiter = '%='

###########################################################
#   Utility Functions
#

def frange(start, end=None, inc=1.0):
    """A range function that accepts both ints and floats."""

    if not end:
        end = start
        start = 0

    count = int( math.ceil((end - start) / inc) )

    L = [None] * count
    L[0] = start
    for i in xrange(1, count):
        L[i] = L[i-1] + inc

    return L


