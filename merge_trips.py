#This script merges trips when there is only short time between the previous and the next trip

import pyparade
import operator

import util, config

MIN_STOP_TIME = 30 #number of minutes that have to be between trips in order to not be merged

queries = {
	"create_trip": "SQL/create_trip.sql",
	"merge_trips": "SQL/merge_trips.sql",
}


def merge_trips(user_id, cur):
	"""Merges trips for one user and uploads to target trip table, trips that don't need to be merged are inserted unchanged
	Args:
		user_id: the user_id for which trips should be merged
	"""

	#fetch all trip times for user
	cur.execute("SELECT id, EXTRACT (EPOCH FROM start_time), EXTRACT (EPOCH FROM end_time) \
				FROM %(trip)t \
				WHERE user_id = %(user_id)s \
				ORDER BY start_time ASC", {"user_id": user_id})

	trip_group_ids = []

	previous_trip = None
	current_group = []
	for trip in cur:
		if not previous_trip == None and trip[1] >= previous_trip[2] + MIN_STOP_TIME*60: #finish group when time gap big enough
			if len(current_group) > 0:
				trip_group_ids.append(current_group)
				current_group = []

		current_group.append(trip[0])
		previous_trip = trip


	single_trip_ids = [group for group in trip_group_ids if len(group) == 1]
	single_trip_ids = [item for sublist in single_trip_ids for item in sublist] #flatten

	merge_trip_ids = [group for group in trip_group_ids if len(group) > 1]

	#Insert single trips unchanged
	print(single_trip_ids)
	cur.execute("INSERT INTO %(trip_target)t SELECT * FROM %(trip)t WHERE id = ANY(%(trip_ids)s)", {"trip_ids": single_trip_ids})

	#Merge grouped trips
	for group in merge_trip_ids:
		cur.execute(open(queries["merge_trips"],'r').read(), {"trip_ids": group})

	return sum([len(group) for group in merge_trip_ids])

if __name__ == '__main__':    
	print("Creating trips table...")
	with util.get_cursor() as cur:
		cur.execute(open(queries["create_trip"], 'r').read(), names = {"trip": config.NAMES["trip_target"]})

	print("Uploading data...")
	user_ids = []
	with util.get_cursor() as cur:
		cur.execute("SELECT user_id FROM %(user_list)t")
		user_ids = [r[0] for r in cur.fetchall()]
	merge_count = pyparade.Dataset(user_ids).map(merge_trips, util.get_cursor).fold(0, operator.add).collect(description="Merging trips")

	print "Merged " + str(merge_count[0]) + " trips."

