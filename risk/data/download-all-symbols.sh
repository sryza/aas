#!/bin/bash
mkdir stocks
while read SYMBOL; do
  ./download-symbol.sh ${SYMBOL} stocks
  sleep 1
done < symbols.txt
