import collections, sys, itertools, multiprocessing, re, datetime, time, threading, os, random, Queue
from contextlib import contextmanager
from exceptions import KeyboardInterrupt, ValueError
#import psycopg2.pool
import psycopg2

import config

if "PASSWORD" in dir(config) == None:
	db_login()
#connpool = psycopg2.pool.ThreadedConnectionPool(min(100,multiprocessing.cpu_count()),100,dbname=config.DATABASE, user=config.USER, password=config.PASSWORD, host="localhost", port=str(config.PORT))

MAXCUR = 2
curpool = Queue.Queue(maxsize = 2)
curcount = 0
curcountlock = threading.Lock()

def parse_trajectory(linestr):
	"""Reads a line from csv file and parses the trajectory
	Args:
		linestr: one-line string from the a STEM csv file
	Returns:
		A tuple (userid, sequence) where sequence is a list of tuples (lat, lon, timestr) for each position in the trajectory"""

	userid, trajectory = re.match(r"([0-9]+)[\s]+\[([\S\s]+)\]",linestr).groups()
	sequence = [] #list of tuples containing userid, lat, lon, time
	for lat, lon, timestr in re.findall(r"\[\[([\-0-9.]+), ([\-0-9.]+)\], '([0-9:\- ]+)'\]",trajectory):
		t = datetime.datetime.strptime(timestr.rpartition("-")[0], "%Y-%m-%d %H:%M:%S")
		sequence.append((float(lat), float(lon), t))
	return (userid, sequence)

def parse_antenna(linestr):
	"""Reads a line from antenna csv file
	Args:
		linestr: one-line string from the a antenna csv file
	Returns:
		A list of tuples (lon, lat, srid) containing the antenna positions extracted, 
		where lat/lon are in the coordinate system given by the SRID; 
		alternatively a tuple (id, lon, lat, srid) if a specific id shoul be assigned to the antenna"""

	lon, lat = re.match(r"([0-9.]+),([0-9.]+)",linestr).groups()
	return (float(lon), float(lat), 32611)

def parse_trip(linestr):
	"""Reads a line from a trip csv file
	Args:
		linestr: one-line string from the a trip csv file
	Returns:
		A list of tuples (userid, cellpath) containing the user id and a list of visited cell on the trip"""

	try:
		data = re.match(r"([0-9]+),([01]),([0-9.]+),([0-9.]+),([0-9 ]*)",linestr).groups()
		userid, commute_direction, orig_TAZ, dest_TAZ, cellpathstr = data
		if int(commute_direction) == 1 or len(cellpathstr) == 0:
			return None #ignore trips for afternoon commute or with empty cellpaths
		try:
			cellpath = [int(cell) for cell in cellpathstr.strip(" ").split(" ")]
			return (userid, cellpath)
		except Exception, e:
			print("Line '" + linestr + "' could will be ignored, because '" + cellpathstr + "' is not a valid cellpath")
			return None
	except Exception, e:
		print("Line '" + linestr + "' has an invalid syntax and will be ignored.")
		return None

def parse_taz(feature):
	"""Parses a geojson feature dict
	Args:
		feature: geojson feature dict
	Returns:
		A tuple (taz_id, pglinestr), where a pglinestr is a postgis linestring describing the TAZ polygon
	"""
	taz_id = int(feature["properties"]["TAZ_ID"])
	linestr = to_pglinestring([(lat, lon) for lon, lat in feature["geometry"]["coordinates"][0]])
	return (taz_id, linestr)

def to_pglinestring(points):
	"""Converts a list of (lat,lon) points to a postgis LINESTRING
	Args:
		points: A list of tuples (lat,lon) describing the points of the LINESTRING
	Returns:
		A postgis LINESTRING following the given points
	"""

	return "LINESTRING (" + ",".join([str(lat) + " " + str(lon) for lon, lat in points]) + ")"

def confirm(prompt_str, allow_empty=False, default=False):
	"""Prompts the user to confirm an action and returns the users decision.
	Args:
		prompt_str:
			A description of the action that the user should confirm (for example "Delete file x?")
		allow_empty:
			If true, the default action assumed, even if the user just pressed enter
		default:
			The default action (true: accept, false: decline)
	Returns:
		True if the user accepted the action and false if not.
	"""

	fmt = (prompt_str, 'y', 'n') if default else (prompt_str, 'n', 'y')
	if allow_empty:
		prompt = '%s [%s]|%s: ' % fmt
	else:
		prompt = '%s %s|%s: ' % fmt
	while True:
		ans = raw_input(prompt).lower()
		if ans == '' and allow_empty:
			return default
		elif ans == 'y':
			return True
		elif ans == 'n':
			return False
		else:
			print("Please enter y or n.")

def chunks(seq, n):
	"""Partionions a sequence into chunks
	Args:
		seq: the sequence to split in chunks
		n: the maximum chunksize
	Return:
		A generator that yields lists containing chunks of the original sequence
	"""

	if n <= 0:
		raise ValueError("Chunksize must be non-negative")

	chunk = []
	for el in seq:
		chunk.append(el)
		if len(chunk) >= n:
			yield chunk
			chunk = []
	if len(chunk) > 0:
		yield chunk

def od_chunks(chunksize = 200):
	"""Returns a generator that returns OD pair chunks based on the cell ids

	Returns:
		A generator that returns tuples of the form ([list of origins], [list of destinations])"""

	for origin in config.CELLS:
		for destinations in chunks(config.CELLS, chunksize):
			yield ([origin], destinations)

def get_random_od_data(limit):
	conn, cur = db_connect()
	sql = "	SELECT orig_cell, dest_cell, interval, flow \
			FROM (SELECT * FROM od ORDER BY random() LIMIT %s) AS od"
	cur.execute(sql, (limit,))
	result = []
	for orig_cell, dest_cell, interval, flow in cur:
		result.append({"interval": interval, "orig_cells": [orig_cell], "dest_cells": [dest_cell], "flow": flow})
	conn.commit()
	return result

def db_login(force_password=False):
	"""Makes sure that config.PASSWORD is set to the database password. 
	If config.PASSWORD is alread defined, this function will not do anything. Otherwise
	it will try to fetch the password from the systems keychain. If no password is stored
	in the keychain yet, the user is prompted to enter the password and optinally store it
	in the system keychain.

	Args:
		force_password: If set to True, the user is prompted even if the password 
		is stored in the keychain (useful if the password needs to be changed
	"""

	if "PASSWORD" in dir(config) != None: #password already set in config.py
		return
	
	import keyring, getpass
	config.PASSWORD = keyring.get_password(config.DATABASE, config.USER)
	if config.PASSWORD == None or force_password == True:
		while 1:
			print("A password is needed to continue. Please enter the password for")
			print(" * service: postgresql")
			print(" * database: " + config.DATABASE)
			print(" * user: " + config.USER)
			print("to continue.")
			config.PASSWORD = getpass.getpass("Please enter the password:\n")
			if config.PASSWORD != "":
				break
			else:
				print ("Authorization failed (no password entered).")
		# store the password
		if confirm("Do you want to securely store the password in the keyring of your operating system?",default=True):
			keyring.set_password(config.DATABASE, config.USER, config.PASSWORD)
			print("Password has been stored. You will not have to enter it again the next time. If you need to edit the password use the keychain manager of your system.")

def partition(mapped_values):
	"""Organize the mapped values by their key.
	Returns an unsorted sequence of tuples with a key and a sequence of values.

	Args: 
		mapped_values: a list of tuples containing key, value pairs

	Returns:
		A list of tuples (key, [list of values])
	"""

	partitioned_data = collections.defaultdict(list)
	for key, value in mapped_values:
		partitioned_data[key].append(value)
	return partitioned_data.items()

class MapReduce(object):
	def __init__(self, map_func, reduce_func, num_workers=multiprocessing.cpu_count(), initializer = None):
		"""
		map_func

		  Function to map inputs to intermediate data. Takes as
		  argument one input value and returns a tuple with the key
		  and a value to be reduced.
		
		reduce_func

		  Function to reduce partitioned version of intermediate data
		  to final output. Takes as argument a key as produced by
		  map_func and a sequence of the values associated with that
		  key.
		 
		num_workers

		  The number of workers to create in the pool. Defaults to the
		  number of CPUs available on the current host.
		"""
		self.map_func = map_func
		self.reduce_func = reduce_func
		self.mappool = multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)
		self.reducepool = multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)
		self.request_stop = False
		self.num_workers = num_workers
		self.enqueued = 0

	def stop(self):
		self.request_stop = True
		self.mappool.terminate()
		self.reducepool.terminate()

	def xinputs(self, inputs):
		for value in inputs:
			while self.enqueued - self.tasks_finished > 100*self.chunksize:
				time.sleep(1)
			self.enqueued += 1
			if self.request_stop:
				raise KeyboardInterrupt("Abort requested")
			yield value
	
	def __call__(self, inputs, chunksize=10, pipe=False, length = None, out = True):
		"""Process the inputs through the map and reduce functions given. Don't call one MapReducer from different threads,
		as it is not thread-safe.
		
		inputs:
		  An iterable containing the input data to be processed.
		chunksize:
		  The portion of the input data to hand to each worker.  This
		  can be used to tune performance during the mapping phase.
		pipe:
		  When set to true, key/value pairs are passed from map directly to reduce function just once. 
		  Only applicable, when all values for every key are generated at once (no partioning or 
		  reducing of the result of reduce)
		length:
			The length of the input iterable for the status indicator. If None, len(inputs) is used.
		out:
			The result is returned as output by default. If out=True, an empty list is returned (if the result is irrelevant and 
			only the side effects of the map/reduce functions are desired). 

		Returns:
			A list containing the resulting tuples (key, value).
		"""

		self.chunksize = chunksize
		self.enqueued = 0
		self.tasks_finished = 0

		if length == None:
			length = len(inputs)

		#map
		start = time.time()
		result = []
		mapped = []
		for response in self.mappool.imap_unordered(self.map_func, self.xinputs(inputs), chunksize=chunksize):
			if pipe:
				mapped.extend(response)
			else:
				result.extend(response)
			if self.request_stop:
				raise KeyboardInterrupt("Abort requested")

			self.tasks_finished += 1

			if self.tasks_finished % (chunksize*self.num_workers) == 0:
				#partition
				partitioned_data = []
				if pipe:
					partitioned_data = partition(mapped)
				else:
					partitioned_data = partition(result)
				#reduce
				reduced = self.reducepool.map(self.reduce_func, partitioned_data)
				if self.request_stop:
					raise KeyboardInterrupt("Abort requested")
				if pipe:
					mapped = []

				if out:
					if pipe:
						result.extend(reduced)
						mapped = []
					else:
						result = reduced

				est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-start)/self.tasks_finished*(length-self.tasks_finished))
				sys.stderr.write('\rdone {0:%}'.format(float(self.tasks_finished)/length) + "  ETA " + est.strftime("%Y-%m-%d %H:%M"))

		#partition
		partitioned_data = []
		if pipe:
			partitioned_data = partition(mapped)
		else:
			partitioned_data = partition(result)

		#reduce
		reduced = self.reducepool.map(self.reduce_func, partitioned_data)
		if pipe:
			mapped = []

		if out:
			if pipe:
				result.extend(reduced)
				mapped = []
			else:
				result = reduced

		sys.stderr.write('\rdone 100%                                  ')
		print("")
		return result

def void(arg):
		return arg

class ParMap(MapReduce):
	def __init__(self, map_func, num_workers=multiprocessing.cpu_count(), initializer = None):
		"""
		map_func

		  Function to map inputs to intermediate data. Takes as
		  argument one input value and returns a tuple with the key
		  and a value to be reduced.
		 
		num_workers

		  The number of workers to create in the pool. Defaults to the
		  number of CPUs available on the current host.
		"""
		self.map_func = map_func
		self.mappool = multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)
		self.request_stop = False
		self.num_workers = num_workers
		self.enqueued = 0

	def stop(self):
		self.request_stop = True
		self.mappool.terminate()

	def __call__(self, inputs, chunksize=10, length = None):
		"""Process the inputs through the map and reduce functions given.
		
		inputs
		  An iterable containing the input data to be processed.
		
		chunksize=1
		  The portion of the input data to hand to each worker.  This
		  can be used to tune performance during the mapping phase.
		"""

		self.chunksize = chunksize
		self.enqueued = 0
		self.tasks_finished = 0

		if length == None:
			length = len(inputs)

		#map
		self.tasks_finished = 0
		start = time.time()
		result = []
		for response in self.mappool.imap_unordered(self.map_func, self.xinputs(inputs), chunksize=chunksize):
			result.append(response)
			if self.request_stop:
				raise KeyboardInterrupt("Abort requested")

			self.tasks_finished += 1
			if self.tasks_finished % (chunksize) == 0:
				est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-start)/self.tasks_finished*(length-self.tasks_finished))
				sys.stderr.write('\rdone {0:%}'.format(float(self.tasks_finished)/length) + "  ETA " + est.strftime("%Y-%m-%d %H:%M"))

		sys.stderr.write('\rdone 100%                                  ')
		print("")
		return result

class Timer(object):
	"""measures time"""
	def __init__(self, description):
		super(Timer, self).__init__()
		self.start = time.time()
		self.description = description

	def stop(self):
		self.end = time.time()
		print(self.description + " took " + str(self.end-self.start) + "s.")
		

def exec_command(command):
	import subprocess
	test = subprocess.Popen(command, stdout=subprocess.PIPE)
	return test.communicate()[0]

def get_metadata():
	from datetime import datetime

	now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S (UTC)")
	project_name = exec_command(["git", "rev-parse", "--show-toplevel"])
	revision = exec_command(["git", "rev-parse", "HEAD"])

	s = "=== Created by ===\n"
	s += project_name
	s += "Commit " + revision
	s += "Object created " + now_str + "\n"
	s += "\n"

	s += "=== config.py ===\n"
	with open("config.py") as f:
		s += f.read()
	return s

class DBCursor(object):
	def __init__(self, conn, schema):
		super(DBCursor, self).__init__()
		self.conn = conn
		self.cur = conn.cursor()
		self.schema = schema

	@property
	def rowcount(self):
		return self.cur.rowcount

	def execute(self, operation, parameters = {}, names = {}):
		#print(self.mogrify(operation, parameters, names))
		r = self.cur.execute(self.mogrify(operation, parameters, names))
		self.conn.commit()
		return r

	def fetchone(self):
		return self.cur.fetchone()

	def fetchall(self):
		return self.cur.fetchall()

	def mogrify(self, operation, parameters = {}, names = {}):
		op = operation

		#resolve names
		allnames = {}
		if hasattr(config, "NAMES"):
			allnames.update(config.NAMES)
		allnames.update(names) #names parameter has priority over config.NAMES dict

		for alias, name in allnames.items():
			if not re.match("[a-zA-Z0-9_]+", name):
				raise ValueError(name + " is not a valid name")
			op = re.sub("%\(" + alias + "\)t", name, op)

		match = re.search("%\([a-zA-Z0-9_]+\)t", op)
		if match:
			raise ValueError(match.group(0) + " is not defined in names dict")

		#replace %m with metadata
		op = re.sub("%m", get_metadata(), op)

		#set schema
		if self.schema != None:
			if not re.match("[a-zA-Z0-9_]+", self.schema):
				raise ValueError(self.schema + " is not a valid schema")
			op = "SET search_path TO " + self.schema + ", public;" + op

		return self.cur.mogrify(op, parameters)

	def copy_from(self, *args, **kwargs):
		return self.cur.copy_from(*args, **kwargs)

	def __iter__(self):
		return self.cur.__iter__()

class DBConnection(object):
	def __init__(self, conn, schema = None):
		super(DBConnection, self).__init__()
		self.conn = conn
		self.schema = schema
		self.conn.autocommit = True

	def cursor(self):
		return DBCursor(self.conn, schema = self.schema)

	def commit(self):
		self.conn.commit()

	def rollback(self):
		self.conn.rollback()

	def close(self):
		self.conn.close()

	def notices():
		def fget(self):
			return self.conn.notices
		return locals()
	notices = property(**notices())

def db_connect(key = str(os.getpid()) + str(threading.current_thread().ident), pool = True):
	#global connpool
	
	conn = None
	if "SCHEMA" in dir(config):
		if pool:
			conn = DBConnection(connpool.getconn(key=key), schema = config.SCHEMA)
		else:
			conn = DBConnection(psycopg2.connect(dbname=config.DATABASE, user=config.USER, password=config.PASSWORD, host="localhost", port=str(config.PORT)), schema = config.SCHEMA)
	else:
		if pool:
			conn = DBConnection(connpool.getconn(key=key))
		else:
			conn = DBConnection(psycopg2.connect(dbname=config.DATABASE, user=config.USER, password=config.PASSWORD, host="localhost", port=str(config.PORT)))
	return (conn, conn.cursor())

def free_connection(conn, key = str(os.getpid()) + str(threading.current_thread().ident)):
	global connpool
	connpool.putconn(conn.conn, key = key, close = False)

def free_cursor(cur):
	global curpool, curcount, curcountlock
	try:
		curpool.put_nowait(cur)
	except Queue.Full, e:
		cur.conn.close() #discard connection
		with curcountlock:
			curcount -= 1

# @contextmanager
# def get_cursor():
# 	global curpool, curcount, curcountlock
# 	cur = None
# 	while cur == None:
# 		try:
# 			cur = curpool.get_nowait()
# 		except Queue.Empty, e:
# 			if curcount < MAXCUR:
# 				conn, cur = db_connect(pool = False)
# 				with curcountlock:
# 					curcount += 1
# 			else:
# 				time.sleep(1)

# 	try:
# 		yield cur
# 	finally:
# 		free_cursor(cur)
# 		#free_connection(conn,r)


@contextmanager
def get_cursor():
	conn, cur = db_connect(pool = False)

	try:
		yield cur
	finally:
		free_cursor(cur)

#make sure config.CELLS exsits
# if not hasattr(config, "CELLS"):
# 	try:
# 		conn, cur = db_connect()
# 		cur.execute("SELECT MIN(id) AS min, MAX(id) AS max FROM eant_pos")
# 		mincell, maxcell = cur.fetchone()
# 		config.CELLS = range(mincell, maxcell+1)
# 	except Exception, e:
# 		pass

# #make sure config.TRIPS exsits
# if not hasattr(config, "TRIPS"):
# 	try:
# 		conn, cur = db_connect()
# 		cur.execute("SELECT MIN(id) AS min, MAX(id) AS max FROM trips_original")
# 		mintrip, maxtrip = cur.fetchone()
# 		config.TRIPS = xrange(mintrip, maxtrip+1)
# 	except Exception, e:
# 		pass

# #make sure config.TRIPS exsits
# if not hasattr(config, "INTERVALS"):
# 	try:
# 		conn, cur = db_connect()
# 		cur.execute("SELECT array_agg(DISTINCT interval) FROM taz_od")
# 		config.INTERVALS = cur.fetchone()[0]
# 	except Exception, e:
# 		pass

