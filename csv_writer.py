'''
	File Name: csv_writer.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:
		This script pulls trips from the database, checks for errors, write trips to a JSON
		file and then output to a CSV file.
'''
# Import libraries.
import os
import csv
import json
import requests
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

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
#							Function: Make CSV 							#
#-----------------------------------------------------------------------#
def make_csv(week,day):
	# Open Log and add details.
	logger = crawler_logger_init()
	logger.info("Creating CSV file for week: "+str(week)+" day "+str(day))

	# Create file name.
	MAIN_NAME = "sao-paulo-crawler-"
	INCREMENTAL_FILENAME_SUFFIX = str(week)+"-"+str(day)
	NAME_EXTENSION = ".csv"
	OUTPUT_DIR = "/data/"
	FINAL_NAME = OUTPUT_DIR+MAIN_NAME+INCREMENTAL_FILENAME_SUFFIX+NAME_EXTENSION

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Find all trips for the given day of the week.
	r = db.try0.find({"weeks":week,"timestamp.day":day})
	l = list(r)
	length = len(l)
	logger.info("Number of trips to write to csv today: "+str(length))

	# Initialize number of error crawls.
	error_crawls = 0

	for num in range(length):
		for x in range (0,24):
			for y in range (0,3):
				time_mins = y*20
				time_hours = x
				temp_time1 = "Time.car"+str(time_hours)+"_"+str(time_mins)
				temp_time2 = "Distance.car"+str(time_hours)+"_"+str(time_mins)
				l[num][temp_time1] = "0"
				l[num][temp_time2] = "0"

	for num1 in range(length):
		print("writing " + str(num1))
		temp_hours = l[num1]["timestamp"]["hours"]
		temp_mins = l[num1]["timestamp"]["minutes"]

		if temp_mins < 20:
			temp_mins = 0
		elif temp_mins < 40:
			temp_mins = 20
		else:
			temp_mins = 40

		m120_hours = temp_hours - 2
		m120_mins = temp_mins
		if m120_hours < 0:
			m120_hours = (24 + m120_hours)
		m120_timestring1 = "Time.car"+str(m120_hours)+"_"+str(m120_mins)
		m120_timestring2 = "Distance.car"+str(m120_hours)+"_"+str(m120_mins)

		m100_hours = temp_hours-1
		if ((temp_mins - 40) < 0):
			m100_mins = 60 - (40 - temp_mins)
			m100_hours = m100_hours - 1
		else:
			m100_mins = temp_mins - 40
		if m100_hours < 0:
			m100_hours = (24 + m100_hours)
		m100_timestring1 = "Time.car"+str(m100_hours)+"_"+str(m100_mins)
		m100_timestring2 = "Distance.car"+str(m100_hours)+"_"+str(m100_mins)

		m80_hours = temp_hours-1
		if ((temp_mins - 20) < 0):
			m80_mins = 60 - (20 - temp_mins)
			m80_hours = m80_hours - 1
		else:
			m80_mins = temp_mins - 20
		if m80_hours < 0:
			m80_hours = (24 + m80_hours)
		m80_timestring1 = "Time.car"+str(m80_hours)+"_"+str(m80_mins)
		m80_timestring2 = "Distance.car"+str(m80_hours)+"_"+str(m80_mins)

		m60_hours = temp_hours - 1
		m60_mins = temp_mins
		if m60_hours < 0:
			m60_hours = (24 + m60_hours)
		m60_timestring1 = "Time.car"+str(m60_hours)+"_"+str(m60_mins)
		m60_timestring2 = "Distance.car"+str(m60_hours)+"_"+str(m60_mins)

		m40_hours = temp_hours
		if ((temp_mins - 40) < 0):
			m40_mins = 60 - (40 - temp_mins)
			m40_hours = m40_hours - 1
		else:
			m40_mins = temp_mins - 40
		if m40_hours < 0:
			m40_hours = (24 + m40_hours)
		m40_timestring1 = "Time.car"+str(m40_hours)+"_"+str(m40_mins)
		m40_timestring2 = "Distance.car"+str(m40_hours)+"_"+str(m40_mins)

		m20_hours = temp_hours
		if ((temp_mins - 20) < 0):
			m20_mins = 60 - (20 - temp_mins)
			m20_hours = m20_hours - 1
		else:
			m20_mins = temp_mins - 20
		if m20_hours < 0:
			m20_hours = (24 + m20_hours)
		m20_timestring1 = "Time.car"+str(m20_hours)+"_"+str(m20_mins)
		m20_timestring2 = "Distance.car"+str(m20_hours)+"_"+str(m20_mins)

		t0_hours = temp_hours
		t0_mins = temp_mins
		t0_timestring1 = "Time.car"+str(t0_hours)+"_"+str(t0_mins)
		t0_timestring2 = "Distance.car"+str(t0_hours)+"_"+str(t0_mins)

		p20_hours = temp_hours
		if ((temp_mins + 20) >= 60):
			p20_mins = 20 - (60 - temp_mins)
			p20_hours = temp_hours + 1
		else:
			p20_mins = temp_mins + 20
		if p20_hours >= 24 :
			p20_hours = (p20_hours - 24)
		p20_timestring1 = "Time.car"+str(p20_hours)+"_"+str(p20_mins)
		p20_timestring2 = "Distance.car"+str(p20_hours)+"_"+str(p20_mins)

		p40_hours = temp_hours
		if ((temp_mins + 40) >= 60):
			p40_mins = 40 - (60 - temp_mins)
			p40_hours = p40_hours + 1
		else:
			p40_mins = temp_mins + 40
		if p40_hours >= 24 :
			p40_hours = (p40_hours - 24)
		p40_timestring1 = "Time.car"+str(p40_hours)+"_"+str(p40_mins)
		p40_timestring2 = "Distance.car"+str(p40_hours)+"_"+str(p40_mins)

		p60_hours = temp_hours + 1
		p60_mins = temp_mins
		if p60_hours >= 24 :
			p60_hours = (p60_hours - 24)
		p60_timestring1 = "Time.car"+str(p60_hours)+"_"+str(p60_mins)
		p60_timestring2 = "Distance.car"+str(p60_hours)+"_"+str(p60_mins)

		p80_hours = temp_hours + 1
		if ((temp_mins + 20) >= 60):
			p80_mins = 20 - (60 - temp_mins)
			p80_hours = p80_hours + 1
		else:
			p80_mins = temp_mins + 20
		if p80_hours >= 24 :
			p80_hours = (p80_hours - 24)
		p80_timestring1 = "Time.car"+str(p80_hours)+"_"+str(p80_mins)
		p80_timestring2 = "Distance.car"+str(p80_hours)+"_"+str(p80_mins)

		p100_hours = temp_hours + 1
		if ((temp_mins + 40) >= 60):
			p100_mins = 40 - (60 - temp_mins)
			p100_hours = p100_hours + 1
		else:
			p100_mins = temp_mins + 40
		if p100_hours >= 24 :
			p100_hours = (p100_hours - 24)
		p100_timestring1 = "Time.car"+str(p100_hours)+"_"+str(p100_mins)
		p100_timestring2 = "Distance.car"+str(p100_hours)+"_"+str(p100_mins)

		p120_hours = temp_hours + 2
		p120_mins = temp_mins
		if p120_hours >= 24 :
			p120_hours = (p120_hours - 24)
		p120_timestring1 = "Time.car"+str(p120_hours)+"_"+str(p120_mins)
		p120_timestring2 = "Distance.car"+str(p120_hours)+"_"+str(p120_mins)

		l[num1][m20_timestring1] = l[num1]["m20"]["traffic"]
		l[num1][m40_timestring1] = l[num1]["m40"]["traffic"]
		l[num1][m60_timestring1] = l[num1]["m60"]["traffic"]
		l[num1][m80_timestring1] = l[num1]["m80"]["traffic"]
		l[num1][m100_timestring1] = l[num1]["m100"]["traffic"]
		l[num1][m120_timestring1] = l[num1]["m120"]["traffic"]
		l[num1][t0_timestring1] = l[num1]["t0"]["traffic"]
		l[num1][p20_timestring1] = l[num1]["p20"]["traffic"]
		l[num1][p40_timestring1] = l[num1]["p40"]["traffic"]
		l[num1][p60_timestring1] = l[num1]["p60"]["traffic"]
		l[num1][p80_timestring1] = l[num1]["p80"]["traffic"]
		l[num1][p100_timestring1] = l[num1]["p100"]["traffic"]
		l[num1][p120_timestring1] = l[num1]["p120"]["traffic"]

		l[num1][m20_timestring2] = l[num1]["m20"]["distance"]
		l[num1][m40_timestring2] = l[num1]["m40"]["distance"]
		l[num1][m60_timestring2] = l[num1]["m60"]["distance"]
		l[num1][m80_timestring2] = l[num1]["m80"]["distance"]
		l[num1][m100_timestring2] = l[num1]["m100"]["distance"]
		l[num1][m120_timestring2] = l[num1]["m120"]["distance"]
		l[num1][t0_timestring2] = l[num1]["t0"]["distance"]
		l[num1][p20_timestring2] = l[num1]["p20"]["distance"]
		l[num1][p40_timestring2] = l[num1]["p40"]["distance"]
		l[num1][p60_timestring2] = l[num1]["p60"]["distance"]
		l[num1][p80_timestring2] = l[num1]["p80"]["distance"]
		l[num1][p100_timestring2] = l[num1]["p100"]["distance"]
		l[num1][p120_timestring2] = l[num1]["p120"]["distance"]

	ljson = dumps(l,sort_keys = True, indent = 4, separators = (',',':'))
	f = open('json_from_db.json', 'w')
	f.write(ljson)
	f.close()

	file = open('json_from_db.json','r')
	x = json.loads(file.read())
	#import pdb;pdb.set_trace()

	f = open(FINAL_NAME, "ab+")
	z = csv.writer(f)
	z.writerow(["city","survey","trip_id","weeks","origin_latitude","origin_longitude","destination_latitude","destination_longitude","timestamp_day","timestamp_hours","timestamp_minutes","walking_distance","walking_time","biking_distance","biking_time","public_transit_distance","public_transit_time","Distance.car0_00","Time.car0_00","Distance.car0_20","Time.car0_20","Distance.car0_40","Time.car0_40","Distance.car1_00","Time.car1_00","Distance.car1_20","Time.car1_20","Distance.car1_40","Time.car1_40","Distance.car2_00","Time.car2_00","Distance.car2_20","Time.car2_20","Distance.car2_40","Time.car2_40","Distance.car3_00","Time.car3_00","Distance.car3_20","Time.car3_20","Distance.car3_40","Time.car3_40","Distance.car4_00","Time.car4_00","Distance.car4_20","Time.car4_20","Distance.car4_40","Time.car4_40","Distance.car5_00","Time.car5_00","Distance.car5_20","Time.car5_20","Distance.car5_40","Time.car5_40","Distance.car6_00","Time.car6_00","Distance.car6_20","Time.car6_20","Distance.car6_40","Time.car6_40","Distance.car7_00","Time.car7_00","Distance.car7_20","Time.car7_20","Distance.car7_40","Time.car7_40","Distance.car8_00","Time.car8_00","Distance.car8_20","Time.car8_20","Distance.car8_40","Time.car8_40","Distance.car9_00","Time.car9_00","Distance.car9_20","Time.car9_20","Distance.car9_40","Time.car9_40","Distance.car10_00","Time.car10_00","Distance.car10_20","Time.car10_20","Distance.car10_40","Time.car10_40","Distance.car11_00","Time.car11_00","Distance.car11_20","Time.car11_20","Distance.car11_40","Time.car11_40","Distance.car12_00","Time.car12_00","Distance.car12_20","Time.car12_20","Distance.car12_40","Time.car12_40","Distance.car13_00","Time.car13_00","Distance.car13_20","Time.car13_20","Distance.car13_40","Time.car13_40","Distance.car14_00","Time.car14_00","Distance.car14_20","Time.car14_20","Distance.car14_40","Time.car14_40","Distance.car15_00","Time.car15_00","Distance.car15_20","Time.car15_20","Distance.car15_40","Time.car15_40","Distance.car16_00","Time.car16_00","Distance.car16_20","Time.car16_20","Distance.car16_40","Time.car16_40","Distance.car17_00","Time.car17_00","Distance.car17_20","Time.car17_20","Distance.car17_40","Time.car17_40","Distance.car18_00","Time.car18_00","Distance.car18_20","Time.car18_20","Distance.car18_40","Time.car18_40","Distance.car19_00","Time.car19_00","Distance.car19_20","Time.car19_20","Distance.car19_40","Time.car19_40","Distance.car20_00","Time.car20_00","Distance.car20_20","Time.car20_20","Distance.car20_40","Time.car20_40","Distance.car21_00","Time.car21_00","Distance.car21_20","Time.car21_20","Distance.car21_40","Time.car21_40","Distance.car22_00","Time.car22_00","Distance.car22_20","Time.car22_20","Distance.car22_40","Time.car22_40","Distance.car23_00","Time.car23_00","Distance.car23_20","Time.car23_20","Distance.car23_40","Time.car23_40"])

	for index in x:
		z.writerow([index["city"],index["survey"],index["trip_id"],index["weeks"],index["origin"]["latitude"],index["origin"]["longitude"],index["destination"]["latitude"],index["destination"]["longitude"],index["timestamp"]["day"],index["timestamp"]["hours"],index["timestamp"]["minutes"],index["walking"]["distance"],index["walking"]["time"],index["biking"]["distance"],index["biking"]["time"],index["public_transit"]["distance"],index["public_transit"]["time"],index["Distance.car0_0"],index["Time.car0_0"],index["Distance.car0_20"],index["Time.car0_20"],index["Distance.car0_40"],index["Time.car0_40"],index["Distance.car1_0"],index["Time.car1_0"],index["Distance.car1_20"],index["Time.car1_20"],index["Distance.car1_40"],index["Time.car1_40"],index["Distance.car2_0"],index["Time.car2_0"],index["Distance.car2_20"],index["Time.car2_20"],index["Distance.car2_40"],index["Time.car2_40"],index["Distance.car3_0"],index["Time.car3_0"],index["Distance.car3_20"],index["Time.car3_20"],index["Distance.car3_40"],index["Time.car3_40"],index["Distance.car4_0"],index["Time.car4_0"],index["Distance.car4_20"],index["Time.car4_20"],index["Distance.car4_40"],index["Time.car4_40"],index["Distance.car5_0"],index["Time.car5_0"],index["Distance.car5_20"],index["Time.car5_20"],index["Distance.car5_40"],index["Time.car5_40"],index["Distance.car6_0"],index["Time.car6_0"],index["Distance.car6_20"],index["Time.car6_20"],index["Distance.car6_40"],index["Time.car6_40"],index["Distance.car7_0"],index["Time.car7_0"],index["Distance.car7_20"],index["Time.car7_20"],index["Distance.car7_40"],index["Time.car7_40"],index["Distance.car8_0"],index["Time.car8_0"],index["Distance.car8_20"],index["Time.car8_20"],index["Distance.car8_40"],index["Time.car8_40"],index["Distance.car9_0"],index["Time.car9_0"],index["Distance.car9_20"],index["Time.car9_20"],index["Distance.car9_40"],index["Time.car9_40"],index["Distance.car10_0"],index["Time.car10_0"],index["Distance.car10_20"],index["Time.car10_20"],index["Distance.car10_40"],index["Time.car10_40"],index["Distance.car11_0"],index["Time.car11_0"],index["Distance.car11_20"],index["Time.car11_20"],index["Distance.car11_40"],index["Time.car11_40"],index["Distance.car12_0"],index["Time.car12_0"],index["Distance.car12_20"],index["Time.car12_20"],index["Distance.car12_40"],index["Time.car12_40"],index["Distance.car13_0"],index["Time.car13_0"],index["Distance.car13_20"],index["Time.car13_20"],index["Distance.car13_40"],index["Time.car13_40"],index["Distance.car14_0"],index["Time.car14_0"],index["Distance.car14_20"],index["Time.car14_20"],index["Distance.car14_40"],index["Time.car14_40"],index["Distance.car15_0"],index["Time.car15_0"],index["Distance.car15_20"],index["Time.car15_20"],index["Distance.car15_40"],index["Time.car15_40"],index["Distance.car16_0"],index["Time.car16_0"],index["Distance.car16_20"],index["Time.car16_20"],index["Distance.car16_40"],index["Time.car16_40"],index["Distance.car17_0"],index["Time.car17_0"],index["Distance.car17_20"],index["Time.car17_20"],index["Distance.car17_40"],index["Time.car17_40"],index["Distance.car18_0"],index["Time.car18_0"],index["Distance.car18_20"],index["Time.car18_20"],index["Distance.car18_40"],index["Time.car18_40"],index["Distance.car19_0"],index["Time.car19_0"],index["Distance.car19_20"],index["Time.car19_20"],index["Distance.car19_40"],index["Time.car19_40"],index["Distance.car20_0"],index["Time.car20_0"],index["Distance.car20_20"],index["Time.car20_20"],index["Distance.car20_40"],index["Time.car20_40"],index["Distance.car21_0"],index["Time.car21_0"],index["Distance.car21_20"],index["Time.car21_20"],index["Distance.car21_40"],index["Time.car21_40"],index["Distance.car22_0"],index["Time.car22_0"],index["Distance.car22_20"],index["Time.car22_20"],index["Distance.car22_40"],index["Time.car22_40"],index["Distance.car23_0"],index["Time.car23_0"],index["Distance.car23_20"],index["Time.car23_20"],index["Distance.car23_40"],index["Time.car23_40"]])
	f.close()

	print ("Done. CSV File Created for Week: "+str(week)+" Day: "+str(day))

	# Send Slack notification after successfully writing CSV file.
	url = "https://hooks.slack.com/services/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"
	csv_msg = "Sao Paulo 2012 Survey Extended-Crawler: CSV for week-"+str(week)+"-day-"+str(day)+" has been written successfully to the shared drive."
	payload1={"text": csv_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
	except requests.exceptions.RequestException as e:
		logger.info(str("Error while sending Slack Notification 2"))
		logger.info(str(e))
		logger.info(str(csv_msg))
