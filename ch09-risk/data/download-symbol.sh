#!/bin/bash

# Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

wget -O ${2}/${1}.csv "http://www.google.com/finance/historical?q=${1}&startdate=Jan+01%2C+2000&enddate=Dec+31%2C+2013&output=csv"
