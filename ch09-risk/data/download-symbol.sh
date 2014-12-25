#!/bin/bash
curl -o $2/$1.csv http://ichart.yahoo.com/table.csv?s=$1&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv
