#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Copyright 2014 Aaron Smith

#    DEPENDENCY:  Pyrax   -->  https://github.com/rackspace/pyrax

#Use this script to download all files in a container.  It will maintain the directory structure and
#will skip the 0-Byte directory marker files that would cause script to crash.  Using Cyberduck, for
#example, will cause these 0-byte directory markers to be created and cause an issue with container
#download when using tools such as Turbolift or Swiftly.  These are great tools to use, however
#currently they have trouble with the directory marker object which can be traced to an issue with
#the multiprocessing module and the order in which objects get downloaded.  This script mitigates that issue.
#Creates the target cloud container in the current working directory and downloads files into it.
#You can define the number of processes downloading at once (n_processes).  If you are downloading to a
#cloud server in the same datacenter as your cloudfiles then you can set "isPublic" to False and make
#this script use the 'service-net' to download.  You can set the Pyrax module to "debug" output by setting
#'hDebug' to False.

##############################  YOU CAN EDIT THE VARIABLES BELOW THIS LINE     ################################
#Set hDebug to 'True' to debug HTTP calls and see all output.  Set dDebug to 'Flase' so turn off debug output.
hDebug = False
#This will defind the number of threads/processes to spawn
n_processes = 20
#Set to 'True' to use public network, set to 'False' to use service-net
isPublic = True


##############################  DO NOT EDIT BELOW THIS LINE     ################################
try:
	import pyrax
except ImportError as e:
	print e
	print "\n"
	print "Please install Pyrax from 'https://github.com/rackspace/pyrax'."
import os
import sys
import multiprocessing

username = raw_input('Please enter your USERNAME: ')
api_key = raw_input('Please enter your APIKEY: ')
my_region = raw_input("Please enter the container REGION: ")
my_container = raw_input("Please enter the name of the CONTAINER: ")

#Set identity
pyrax.set_setting('identity_type', 'rackspace')
pyrax.set_credentials(username, api_key, authenticate=True)
pyrax.set_http_debug(hDebug)
pyrax.set_default_region(my_region)
pyrax.settings.set("region",my_region)
cfiles = pyrax.connect_to_cloudfiles(region=my_region, public=isPublic)

cont = cfiles.get_container(my_container)
download_me = cont.get_object_names(full_listing=True)
#list_to_download = cont.get_objects(limit=100)
#download_me = [obj.name for obj in list_to_download]

#Create directory to represent top level container
if not os.path.exists(my_container):
	try:
		os.makedirs(my_container)
	except Exception as e:
		print e
		sys.exit(1)

def download_obj(filename):
	try:
		if (cont.get_object(filename).content_type.lower() == "application/directory"):
			print "This is a directory object [%s].  Skipping download!" % filename
		else:
			cont.download_object(filename, my_container, structure=True)
			print 'downloaded object: ', filename
		return
	except Exception as e:
		print "failed to download object: ", filename
		print 'ERROR BELOW'
		print e
		return

try:
	pool = multiprocessing.Pool(processes=n_processes)
	pool.map(download_obj, download_me)
	pool.close()
	pool.join()
except Exception as e:
	print e

print "All Done!"

