#!/bin/sh

while /bin/true
do
   echo "Done sleeping.  Starting ferry-user-update.py run..."
   python36 /home/ferry/ferry-user-update.py -c /home/ferry/ferry-user-update.config
   echo "Done with ferry-user-update.py run.  Sleeping...."
   sleep 3600
done

