#!/bin/bash

pip3 install -r requirements.txt

if [ -x "/usr/local/bin/Rscript" ]
then
   Rscript -e "install.packages('jsonlite')"
   Rscript -e "install.packages('cml')"
fi