'''
	File Name: scheduler.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:
		This script pulls data for the week from the database. The trips are converted to
		server time, replicated for all variations (m120 to t0 to p120) and scheduled for
		crawls. The crawler is called for every trip. The call to the CSV creator is
		scheduled at 6:01am server time, 1:01am central time everyday for the days trips.
		The script then returns control to the controller on Saturday at 7:00am server time,
		so 2:00am central time.
'''

# Import libraries.
import crawler
import csv_writer
import schedule
import datetime
import requests
import logging
import time
import json
import copy
import os
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient

# Global variable to keep track of scheduling loop.
schd_bool = 0

#-----------------------------------------------------------------------#
#						Function: Crawler Logger Init    				#
#-----------------------------------------------------------------------#
def crawler_logger_init():
	# Create log.
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)

	# Create the log handler & reset every week.
	lh = logging.FileHandler("extended_crawler_log.txt")

	# Format the log.
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	lh.setFormatter(formatter)

	# Add handler to the logger object.
	logger.addHandler(lh)
	return logger

#-----------------------------------------------------------------------#
#						Function: Schedule Trips						#
#-----------------------------------------------------------------------#
def schedule_trips(week, logger):
	# Open Log.
	logger = crawler_logger_init()
	logger.info("Scheduling trips for week: "+str(week))
	print("Scheduling trips for week: " + str(week))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0

	# Resetting global variable to 0, from week 2.
	global schd_bool
	schd_bool = 0

	# Pull a list of trips for the week from the database.
	r = db.try0.find({"weeks":week})
	trip_list = list(r)
	logger.info("Pulled all trips from database")

	# Loop over all trips in the list and change them to server time (accounting for brazil time).
	original_length = len(trip_list)
	for server_time_trip in range(original_length):
		trip_list[server_time_trip]['timestamp']['hours'] = trip_list[server_time_trip]['timestamp']['hours'] + 2
		if trip_list[server_time_trip]['timestamp']['hours'] >= 24:
			trip_list[server_time_trip]['timestamp']['hours'] = trip_list[server_time_trip]['timestamp']['hours'] - 24
			trip_list[server_time_trip]['timestamp']['day'] = trip_list[server_time_trip]['timestamp']['day'] + 1

	# Loop over all trips in the list replicating each trip 15 times to create an instance of each variation.
	for num in range(original_length):
		# m120 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m120"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-2
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# m100 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m100"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		if ((t_obj['timestamp']['minutes'] - 40) < 0):
			t_obj['timestamp']['minutes'] = 60 - (40 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] - 40
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# m80 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m80"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		if ((t_obj['timestamp']['minutes'] - 20) < 0):
			t_obj['timestamp']['minutes'] = 60 - (20 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] - 20
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# m60 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m60"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# m40 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m40"
		if ((t_obj['timestamp']['minutes'] - 40) < 0):
			t_obj['timestamp']['minutes'] = 60 - (40 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours']-1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] - 40
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# m20 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "m20"
		if ((t_obj['timestamp']['minutes'] - 20) < 0):
			t_obj['timestamp']['minutes'] = 60 - (20 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] - 1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] - 20
		if t_obj['timestamp']['hours'] < 0:
			t_obj['timestamp']['hours'] = (24 + t_obj['timestamp']['hours'])
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] - 1
		trip_list.append(t_obj)

		# p20 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p20"
		if ((t_obj['timestamp']['minutes'] + 20) >= 60):
			t_obj['timestamp']['minutes'] = 20 - (60 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] + 20
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# p40 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p40"
		if ((t_obj['timestamp']['minutes'] + 40) >= 60):
			t_obj['timestamp']['minutes'] = 40 - (60 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] + 40
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# p60 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p60"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# p80 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p80"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		if ((t_obj['timestamp']['minutes'] + 20) >= 60):
			t_obj['timestamp']['minutes'] = 20 - (60 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] + 20
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# p100 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p100"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		if ((t_obj['timestamp']['minutes'] + 40) >= 60):
			t_obj['timestamp']['minutes'] = 40 - (60 - t_obj['timestamp']['minutes'])
			t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 1
		else:
			t_obj['timestamp']['minutes'] = t_obj['timestamp']['minutes'] + 40
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# p120 variation
		t_obj = copy.deepcopy(trip_list[num])
		t_obj['mode'] = "driving"
		t_obj['travel_type'] = "p120"
		t_obj['timestamp']['hours'] = t_obj['timestamp']['hours'] + 2
		if t_obj['timestamp']['hours'] >= 24 :
			t_obj['timestamp']['hours'] = (t_obj['timestamp']['hours'] - 24)
			t_obj['timestamp']['day'] = t_obj['timestamp']['day'] + 1
		trip_list.append(t_obj)

		# t0-driving variation
		trip_list[num]['mode'] = "driving"
		trip_list[num]['travel_type'] = "t0"

	logger.info("Number of trips after replication: " + str(len(trip_list)))
	logger.info("Replicated all trips")
	#-----------------------------------------------------------------------#
	#						Function: Crawl 								#
	#-----------------------------------------------------------------------#
	def crawl(trip):
		# Call crawler and crawl trip.
		print ("Calling the Crawler")
		crawler.crawl_trip(trip)
		print ("Crawled Successfully")
		return schedule.CancelJob

	#-----------------------------------------------------------------------#
	#						Function: CSV Creator							#
	#-----------------------------------------------------------------------#
	def csv_creator(week,day):
		# Call csv_writer and create CSV.
		logger.info("Calling CSV Writer")
		csv_writer.make_csv(week,day)
		logger.info("CSV Written")
		return schedule.CancelJob

	#-----------------------------------------------------------------------#
	#						Function: Finish Scheduler						#
	#-----------------------------------------------------------------------#
	def finish_scheduler():
		# Return control to controller.
		logger.info("Bye. Stopping Scheduler.")
		global schd_bool
		schd_bool = 1
		return schedule.CancelJob

	# Schedule all instances for every trip.
	for item in range(len(trip_list)):
		s_time = str(trip_list[item]['timestamp']['hours'])+":"+str(trip_list[item]['timestamp']['minutes'])
		if trip_list[item]['timestamp']['day'] == 0:
			schedule.every().monday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day'] == 1:
		 	schedule.every().tuesday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day'] == 2:
		 	schedule.every().wednesday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day'] == 3:
		 	schedule.every().thursday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day'] == 4:
			schedule.every().friday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day'] == 5:
		 	schedule.every().saturday.at(s_time).do(crawl,trip_list[item])

	# Schedule writing CSV at 3:01am from tuesday to saturday.
	schedule.every().tuesday.at("6:01").do(csv_creator,week,0)
	schedule.every().wednesday.at("6:01").do(csv_creator,week,1)
	schedule.every().thursday.at("6:01").do(csv_creator,week,2)
	schedule.every().friday.at("6:01").do(csv_creator,week,3)
	schedule.every().saturday.at("6:01").do(csv_creator,week,4)
	schedule.every().saturday.at("7:00").do(finish_scheduler)
	logger.info("Scheduled All Trips")

	# Send notification to Slack.
	slack_url = "https://hooks.slack.com/services/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"
	schedule_trips_msg = "Sao Paulo 2012 Survey Extended-Crawler: Scheduling trips succesful."
	payload={"text": schedule_trips_msg}
	try:
		r = requests.post(slack_url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Extended-Crawler: Error while sending scheduler Slack notification.")
		logger.info(e)
		logger.info(schedule_trips_msg)

	# Loop till all trips crawled.
	# while True and schd_bool==0:
	# 	schedule.run_pending()
	# 	time.sleep(1)
