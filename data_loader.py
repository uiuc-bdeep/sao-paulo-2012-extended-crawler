'''
	File Name: data_loader.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script parses data from the CSV Trip Survey and creates a random sample of 
		20 Percent and then creates a JSON file for each day of the week.
		A week number (string), city and survey year are added to the JSON Objects.
		A datestamp for every day of the week is also added.
		The JSON files are then uploaded to a MongoDB database.
'''

# Import libraries.
import os
import csv
import json
import random
import time
import datetime
import logging
import requests
from pymongo import MongoClient

#-----------------------------------------------------------------------#
#							Function: Load Data							#
#-----------------------------------------------------------------------#
def load_data(week_number):
	# Open Log and log date.
	logger = logging.getLogger("extended_crawler.data_loader")
	logger.info("Loading data for week: "+str(week_number))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0
	
	# Open CSV file, read headers and get length of data.
	dataFile = open('congestion_survey.csv')
	traffic_data_sheet = csv.reader(dataFile)
	headers = traffic_data_sheet.next()
	traffic_data_array = list(traffic_data_sheet)

	# Set keys to headers.
	keys = {}
	keys['ID_ORDEM'] = headers.index('ID_ORDEM')
	keys['TIPOVG'] = headers.index('TIPOVG')
	keys['H_SAIDA'] = headers.index('H_SAIDA')
	keys['MIN_SAIDA'] = headers.index('MIN_SAIDA')
	keys['DIA_SEM'] = headers.index('DIA_SEM')
	keys['Lat_O'] = headers.index('Lat_O')
	keys['Long_O'] = headers.index('Long_O')
	keys['Lat_D'] = headers.index('Lat_D')
	keys['Long_D'] = headers.index('Long_D')

	# Create lists for JSON Objects.
	formatted_data = []
	formatted_data_monday = []
	formatted_data_tuesday = []
	formatted_data_wednesday = []
	formatted_data_thursday = []
	formatted_data_friday = []

	# Create datestamps for trips.
	current_date = datetime.datetime.today().strftime('%Y/%m/%d')
	current_date_str = datetime.datetime.strptime(current_date, '%Y/%m/%d')
	week_date = current_date_str + datetime.timedelta(days=+1)
	monday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+2)
	tuesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+3)
	wednesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+4)
	thursday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+5)
	friday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)

	# Create a JSON Object for every trip in the CSV file.
	for i in range(len(traffic_data_array)):
		value = traffic_data_array[i]

		# Exclude trips without timestamps in the CSV file.
		if value[keys['MIN_SAIDA']] == '' or value[keys['DIA_SEM']] == '' or value[keys['H_SAIDA']] == '' or int(value[keys['DIA_SEM']]) == None or int(value[keys['DIA_SEM']]) == 0:
			continue

		# Set datestamp for each trip.
		if (int(value[keys['DIA_SEM']]) - 2)==0:
			datestamp = monday
		if (int(value[keys['DIA_SEM']]) - 2)==1:
			datestamp = tuesday
		if (int(value[keys['DIA_SEM']]) - 2)==2:
			datestamp = wednesday
		if (int(value[keys['DIA_SEM']]) - 2)==3:
			datestamp = thursday
		if (int(value[keys['DIA_SEM']]) - 2)==4:
			datestamp = friday

		traffic_data_dict = {
			"trip_id": str(value[keys['ID_ORDEM']]),
			"survey":"2012",
			"city":"Sao Paulo",
			"weeks": week_number,
			# Week starts at day 2 (aka Monday == 2) in the CSV file, we start it at 0.
			"timestamp": {
				"hours": int(value[keys['H_SAIDA']]),
				"minutes": int(value[keys['MIN_SAIDA']]),
				"day": int(value[keys['DIA_SEM']]) - 2
			},
			"origin": {
				"latitude": value[keys['Lat_O']],
				"longitude": value[keys['Long_O']]
			},
			"destination": {
				"latitude": value[keys['Lat_D']],
				"longitude": value[keys['Long_D']]
			},
			"public_transit": {
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"biking":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"walking":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"m120":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},   	# minus 120
			"m100":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"m80":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			
			},
			"m60":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"m40":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"m20":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"t0":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"p20":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},   	# plus 20
			"p40":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"p60":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"p80":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"p100":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			},
			"p120":{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			}
		}
		
		# Append every JSON trip into the list.
		formatted_data.append(traffic_data_dict)

		# Append every JSON trip into the list for the respective day.
		if int(value[keys['DIA_SEM']]) - 2 == 0:
			formatted_data_monday.append(traffic_data_dict)
		elif (int(value[keys['DIA_SEM']]) - 2)==1:
			formatted_data_tuesday.append(traffic_data_dict)
		elif (int(value[keys['DIA_SEM']]) - 2)==2:
			formatted_data_wednesday.append(traffic_data_dict)
		elif (int(value[keys['DIA_SEM']]) - 2)==3:
			formatted_data_thursday.append(traffic_data_dict)
		elif (int(value[keys['DIA_SEM']]) - 2)==4:
			formatted_data_friday.append(traffic_data_dict)

	# Close the CSV file.
	dataFile.close()

	# Log data statistics.
	logger.info("Parsed CSV file and created JSON Objects")
	logger.info("Total number of trips for this week: " + str(len(formatted_data)))
	logger.info("Total number of trips for Monday: " + str(len(formatted_data_monday)))
	logger.info("Total number of trips for Tuesday: " + str(len(formatted_data_tuesday)))
	logger.info("Total number of trips for Wednesday: " + str(len(formatted_data_wednesday)))
	logger.info("Total number of trips for Thursday: " + str(len(formatted_data_thursday)))
	logger.info("Total number of trips for Friday: " + str(len(formatted_data_friday)))

	# Caluclate number of trips to be extracted per day by the random sample generator.
	monday_count = int(0.2 * len(formatted_data_monday))
	tuesday_count = int(0.2 * len(formatted_data_tuesday))
	wednesday_count = int(0.2 * len(formatted_data_wednesday))
	thursday_count = int(0.2 * len(formatted_data_thursday))
	friday_count = int(0.2 * len(formatted_data_friday))

	# Create random sample of trips for every day.
	monday_rand_items = random.sample(formatted_data_monday, monday_count)
	tuesday_rand_items = random.sample(formatted_data_tuesday, tuesday_count)
	wednesday_rand_items = random.sample(formatted_data_wednesday, wednesday_count)
	thursday_rand_items = random.sample(formatted_data_thursday, thursday_count)
	friday_rand_items = random.sample(formatted_data_friday, friday_count)

	# Log data statistics.
	logger.info("Created random sample")
	logger.info("20 percent of the Trips for Monday: " + str(len(monday_rand_items)))
	logger.info("20 percent of the Trips for Tuesday: " + str(len(tuesday_rand_items)))
	logger.info("20 percent of the Trips for Wednesday: " + str(len(wednesday_rand_items)))
	logger.info("20 percent of the Trips for Thursday: " + str(len(thursday_rand_items)))
	logger.info("20 percent of the Trips for Friday: " + str(len(friday_rand_items)))

	# Write all JSON Objects to a JSON file. JSON file only contains all trips of the current day.
	body_monday_rand_items = json.dumps(monday_rand_items, sort_keys = True, indent = 4, separators = (',',':'))
	body_tuesday_rand_items = json.dumps(tuesday_rand_items, sort_keys = True, indent = 4, separators = (',',':'))
	body_wednesday_rand_items = json.dumps(wednesday_rand_items, sort_keys = True, indent = 4, separators = (',',':'))
	body_thursday_rand_items = json.dumps(thursday_rand_items, sort_keys = True, indent = 4, separators = (',',':'))
	body_friday_rand_items = json.dumps(friday_rand_items, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('20percent_monday.json', 'w')  
	f.write(body_monday_rand_items)
	f.close()
	f = open('20percent_tuesday.json', 'w')  
	f.write(body_tuesday_rand_items)
	f.close()
	f = open('20percent_wednesday.json', 'w')  
	f.write(body_wednesday_rand_items)
	f.close()
	f = open('20percent_thursday.json', 'w')  
	f.write(body_thursday_rand_items)
	f.close()
	f = open('20percent_friday.json', 'w')  
	f.write(body_friday_rand_items)
	f.close()
	logger.info("Wrote JSON files containing random sample of trips.")

	# Push JSON Objects from the file into the database.	
	page = open("20percent_monday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("20percent_tuesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("20percent_wednesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("20percent_thursday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("20percent_friday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	logger.info("Loaded data into the database.")

	# Send notification to Slack.
	url = "https://hooks.slack.com/services/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"
	data_loader_msg = "Sao Paulo 2012 Survey Extended-Crawler: Data loading succesful."
	payload={"text": data_loader_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Extended-Crawler: Error while sending data loader Slack notification.")
		logger.info(e)
		logger.info(data_loader_msg)
