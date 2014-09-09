#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Copyright 2014 Aaron Smith

#Use this script to download all files in a container.  It will maintain the directory structure and
#will skip the 0-Byte directory marker files that would cause script to crash.  Using Cyberduck, for
#example, will cause these 0-byte directory markers to be created and cause an issue with container
#download.  This script mitigates that issue.

#Creates the cloud container in the current working directory and downloads files into it.

#dependency:  Pyrax   -->  https://github.com/rackspace/pyrax


import pyrax
import os
import sys
import multiprocessing

username = raw_input('Please enter your USERNAME: ')
api_key = raw_input('Please enter your APIKEY: ')
my_region = raw_input("Please enter the container REGION: ")
my_container = raw_input("Please enter the name of the CONTAINER: ")

#Set hDebug to 'True' to debug HTTP calls and see all output.  Set dDebug to 'Flase' so turn off debug output.
hDebug = False
#This will defind the number of threads/processes to spawn
n_processes = 20
#Set to 'True' to use public network, set to 'False' to use service-net
isPublic = True

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

pool = multiprocessing.Pool(processes=n_processes)
pool.map(download_obj, download_me)
pool.close()
pool.join()

