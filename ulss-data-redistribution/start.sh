#!/bin/bash
cd /home/ulss/project/ulss/ulss-data-redistribution
. ~/.bash_profile

nohup java -jar dist/ulss-data-redistribution.jar -Xms 10000m -Xmx 10000m -Xss 128k &
