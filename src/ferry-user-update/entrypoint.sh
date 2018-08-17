#!/bin/sh

while /bin/true
do
   python36 /home/ferry/ferry-user-update.py -c /home/ferry/ferry-user-update.config
   sleep 3600
done

