# -*- coding: utf-8 -*-
#########################################################################
# File Name: backup.sh
# Author: wangyuan
# mail: wangyuan214159@sogou-inc.com
# Created Time: 三  7/31 15:29:53 2019
#########################################################################
#!/bin/bash


bin=`dirname $0`
cd $bin


BASE=../
DAY=`date +%Y%m%d`

find log/ -mtime +10 -exec rm -rf {} \;
git add .
git commit -m "modify date:"$DAY
git push -u origin master


