#!/bin/bash

# Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

curl -o $2/$1.csv https://ichart.yahoo.com/table.csv?s=$1&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv
