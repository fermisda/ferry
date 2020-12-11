####################################################
# generates a test report from pytest output  and  #
# prints results in csv format                     #
# it is executing pytest test_name                 #
# all test files (test_.*yaml or based on the      #
# specified pattern) in the directory              #
# based on creation time                           #
####################################################


import os
import sys
import time
import glob
import pytest
import argparse

parser = argparse.ArgumentParser(description="Script to run pytest on created taven tests and generate a report in csv format")
parser.add_argument("-p", "--pattern", action="store", help="pattern (substring of API name) for tavern test files, if not provided uses all tests, e.g pattern could be 'get' and then all get* API tests will be executed, if argument not used all tests will be executed ")
opts = parser.parse_args()
table = {}
failures = 0
time_spent = 0.0
if opts.pattern:
      pattern = "test_"+opts.pattern+"*tavern.yaml"
else:
      pattern = 'test_*tavern.yaml'

file_list = glob.glob(pattern)
file_list.sort(key=os.path.getctime)
if not len(file_list):
       print("No files that matche %s have found" % pattern)
       sys.exit(1)

new_target = open(os.devnull, "w")
old_target = sys.stdout
sys.stdout = new_target
# need it to supress pytest output, could make any supress options to work
for f in file_list:
	start_time = time.time()
	exit_code = rep=pytest.main(["--tb=no",  "--disable-warnings", f])
	test_time = time.time() - start_time	
	time_spent += test_time
	result = "success"
	if exit_code:
		failures += 1
		result = "failure"
	table[f] = [result, test_time]


sys.stdout = old_target


print ("Test #,Test,Result,Duration(sec)")

i = 0
for key, value in table.items():
	i += 1
	time_spent += round(float(value[1]),3)
	print ("%s, %s, %s, %s" % (i,key, value[0],value[1]))
print ("Total number of tests: %s,,number of failures: %s, Time spent (min) :%s" % (len(table), failures, round(time_spent/60.,2)))
sys.exit(0)
