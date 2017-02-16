# coding=utf-8

# Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

# CTCF PWM from suppl. to  https://dx.doi.org/10.1016/j.cell.2012.12.009

import numpy as np
import pandas as pd

n = 17
bases = ['A', 'C', 'G', 'T']
raw = {
    'A': '5423    2600    0   64  641 46  5823    87  0   37  10566   90  183 2   406 2660    4213',
    'C': '546 371 8052    0   9591    10107   5201    7091    8872    49  290 1616    5157    0   114 4936    3590',
    'G': '1733    11366   0   12480   8   0   207 0   1   23  1625    9828    0   8472    7463    143 2456',
    'T': '4208    632 505 86  46  0   241 3888    0   14151   1355    764 7952    121 2221    9766    1302'}
counts = [map(float, raw[base].split()) for base in bases]
pseudocounts = pd.DataFrame(counts, index=bases) + 1
pwm = pseudocounts / pseudocounts.sum()

# generate the PWM in Scala
maps = []
for pos in xrange(n):
    pairs = []
    for base in bases:
        pairs.append("'%s'->%.4f" % (base, pwm.ix[base, pos]))
    maps.append('Map(' + ','.join(pairs) + ')')
print 'val pwmData = sc.broadcast(Vector(\n  ' + ',\n  '.join(maps) + '))'
