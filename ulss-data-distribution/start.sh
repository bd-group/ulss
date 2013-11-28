#!/bin/bash
cd /home/ulss/evan/ulss-data-redistribution
. ~/.bash_profile

nohup java -jar dist/ulss-data-redistribution.jar &
