#!/bin/bash

# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

curl -o $2/$1.csv http://ichart.yahoo.com/table.csv?s=$1&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv
