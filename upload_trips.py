import pyparade
import os, os.path
import glob
import xml.etree.ElementTree as ET
import dateutil.parser


import util, config

KML_DIR = "kml"

NS_KML = "{http://www.opengis.net/kml/2.2}"
NS_GX = "{http://www.google.com/kml/ext/2.2}"

def extract_trips_from_kml(path):
	"""Converts kml data into trips and uploads to database. The userid is the folder name.
	Args:
		path: the file to import, relative path insitde KML_DIR
	"""
	trips = []

	try:
		tree = ET.parse(path)
		root = tree.getroot()
		document = root.find(NS_KML + "Document")
		checked_user_id = False
		for placemark in document.findall(NS_KML + "Placemark"):
			try:
				user_id = os.path.split(os.path.split(path)[0])[1]

				data = placemark.find(NS_KML + "ExtendedData")
				distance = int(data.find(NS_KML + "Data[@name='Distance']/" + NS_KML + "value").text)
				
				#skip if standstill
				if distance == 0:
					continue

				category = data.find(NS_KML + "Data[@name='Category']/" + NS_KML + "value").text
				email = data.find(NS_KML + "Data[@name='Email']/" + NS_KML + "value").text
				begin = placemark.find(NS_KML + "TimeSpan/" + NS_KML + "begin").text
				end = placemark.find(NS_KML + "TimeSpan/" + NS_KML + "end").text

				#check if correct user
				moftkts = int(user_id) - 46765510630 + 1
				if moftkts >= 10:
					moftkts = str(moftkts)
				else:
					moftkts = "0" + str(moftkts)
				if (not checked_user_id and email != "moftkts" + moftkts + "@gmail.com"):
					print("WARNING: user id and email not matching for " + path + ". Email is " + email + " where " + "moftkts" + moftkts + "@gmail.com" + " was expected!")
					checked_user_id = True

				gpstrack = placemark.find(NS_GX + "Track")
				gxcoords = [e.text[0:-2] for e in gpstrack.findall(NS_GX + "coord")]

				gps_linestring = "LINESTRING (" + ",".join(gxcoords) + ")"


				tripdata = {
					"user_id": user_id, #parent dir of kml file is user id
					"start_time": dateutil.parser.parse(begin),
					"end_time": dateutil.parser.parse(end),
					"distance": float(distance)/1000,
					"geom": gps_linestring,
					"activity": category
				}

				trips.append(tripdata)
			except Exception as e:
				print "ERROR: Skipped placemark due to parsing error: ", e
	except Exception as e:
		print "ERROR: Skipped corrupted file " + path + ": ", e
	
	return trips


def upload_trip(tripdata, cur):
	"""Uploads a trip to the database.
	Args:
		tripdata: dictonary containing trip information"""

	#print tripdata
	cur.execute("INSERT INTO %(trip)t (user_id, start_time, end_time, distance, activity, geom, start_geom, end_geom)\
					 SELECT %(user_id)s, %(start_time)s, %(end_time)s, %(distance)s, %(activity)s,\
					 ST_LineFromText(%(geom)s, 4326), ST_StartPoint(ST_LineFromText(%(geom)s, 4326)), ST_EndPoint(ST_LineFromText(%(geom)s, 4326))", tripdata)

if __name__ == '__main__':
	files = glob.glob(KML_DIR + '/*/*.kml')
    
	print("Creating trips table...")
	with util.get_cursor() as cur:
		cur.execute(open("SQL/create_trip.sql", 'r').read())

	print("Uploading data...")
	pyparade.Dataset(files).flat_map(extract_trips_from_kml).map(upload_trip, util.get_cursor).collect(description="Uploading trips")
