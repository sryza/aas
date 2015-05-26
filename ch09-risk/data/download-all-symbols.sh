#!/bin/bash

# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

mkdir stocks
while read SYMBOL; do
  ./download-symbol.sh ${SYMBOL} stocks
  sleep 1
done < symbols.txt
