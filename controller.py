'''
	File Name: controller.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)

	Description:
		This script initializes the log, cleans the log and starts
		the extended crawler every week. It calls the data loader and scheduler.

		The log file, original CSV File, and all temporary files are located at:
		ubuntu@141.142.209.140://home/ubuntu/extended_crawler/

		Old logs (every week) are shifted to:
		/data/Congestion/stream:/data/Congestion/stream/extended_crawler/logs/

		The csv output is written to:
		/data/Congestion/stream:/data/Congestion/stream/extended_crawler/

	Database Info:
		DB: trial
		Collection: try0
'''

# Import libraries.
import csv_writer
import data_loader
import scheduler
import os
import sys
import time
import json
from datetime import datetime
import logging
import requests
from pymongo import MongoClient

# Global variable to keep track of week number.
week_number = 91

#-----------------------------------------------------------------------#
#						Function: Extended Crawler Init					#
#-----------------------------------------------------------------------#
def extended_crawler_init():
	# Update week number and convert to string.
	global week_number
	week_number = week_number + 1
	week_parameter = str(week_number)
	# Call data_loader for the week.
	logger.info("Calling Data Loader For Week: "+ week_parameter)
	data_loader.load_data(week_parameter)
	logger.info("Loaded Data for Week: "+ week_parameter)
	# Call scheduler for the week.
	logger.info("Calling Scheduler for Week: "+week_parameter)
	scheduler.schedule_trips(week_parameter, logger)
	logger.info("Crawled all trips for Week: "+week_parameter)

#-----------------------------------------------------------------------#
#						Function: Crawler Logger Init    				#
#-----------------------------------------------------------------------#
def crawler_logger_init():
	# Create log.
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)

	# Create the log handler & reset every week.
	lh = logging.FileHandler("extended_crawler_log.txt", mode="w")

	# Format the log.
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	lh.setFormatter(formatter)

	# Add handler to the logger object.
	logger.addHandler(lh)
	return logger, lh

# Set up database connection.
client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017)
db = client.trial

# Non-stop loop to start weekly crawls.

slack_url = "https://hooks.slack.com/services/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"

schedule_trips_msg = "Sao Paulo 2012 Survey Extended-Crawler: Initiating controller"
payload = {"text": schedule_trips_msg}
try:
	r = requests.post(slack_url, data=json.dumps(payload))
except requests.exceptions.RequestException as e:
	logger.info("Sao Paulo 2012 Survey Extended-Crawler: Error while sending controller Slack notification.")
	logger.info(e)
	logger.info(schedule_trips_msg)

while True:
	Scheduled start time for normal-crawler for every week.
	start_day = "Sunday"
	start_hour = "18"
	start_min = "30"

	# Calculate current time and check if current time = start time.
	now = datetime.now()
	#print now
	current_day = now.strftime("%A")

	# NOTE: comment out if statement here for testing purpose
	if start_day == current_day and start_hour==str(now.hour) and start_min==str(now.minute):
		logger, lh = crawler_logger_init()

		# Call normal crawler.
		logger.info("Starting Extended Crawler")
		print ("Starting Extended Crawler on "+str(now))
		extended_crawler_init()
		print ("Week " + str(week_number) + " Done.")
		logger.info("Week " + str(week_number) + " Done.")

		# Close log.
		logger.removeHandler(lh)

	# Check every 30 seconds.
	time.sleep(30)
