#~ 2018-06-01
#~ plch holds explorer exporter


import configparser
import sqlite3
import psycopg2
#~ import psycopg2.extras
import os
from datetime import datetime


class App:
	
	#~ the constructor
	def __init__(self):
		#~ the local database connection
		self.sqlite_conn = None
		#~ the remote database connection
		self.pgsql_conn = None
		
		#~ open the config file, and parse the options into local vars
		config = configparser.ConfigParser()
		config.read('config.ini')
		self.db_connection_string = config['db']['connection_string']
		self.local_db_connection_string = config['local_db']['connection_string']
		self.itersize = int(config['db']['itersize'])
		
		#~ open the database connections
		#~ TODO:
		#~ if we're going to be using this object as a "long living" one, 
		#~ maybe write a test to see if the connections are open, and if 
		#~ not, loop connection attemts with a reasonable timeout
		self.open_db_connections()
		
		#~ create the table if it doesn't exist
		self.create_local_table()
	
	
	#~ the destructor
	def __del__(self):
		self.close_db_connections()
		print("done.")
	
	def open_db_connections(self):
		#~ connect to the sierra postgresql server
		try:
			self.pgsql_conn = psycopg2.connect(self.db_connection_string)

		except psycopg2.Error as e:
			print("unable to connect to sierra database: %s" % e)

		#~ connect to the local sqlite database
		try:
			self.sqlite_conn = sqlite3.connect(self.local_db_connection_string)
		except sqlite3.Error as e:
			print("unable to connect to local database: %s" % e)
			
			
	def close_db_connections(self):
		print("closing database connections...")
		if self.pgsql_conn:
			if hasattr(self.pgsql_conn, 'close'):
				print("closing pgsql_conn")
				self.pgsql_conn.close()
				
			self.pgsql_conn = None

		if self.sqlite_conn:
			if hasattr(self.sqlite_conn, 'commit'):
				print("commiting pending transactions to sqlite db...")
				self.sqlite_conn.commit()
			
			if hasattr(self.sqlite_conn, 'close'):
				print("closing sqlite_conn")
				self.sqlite_conn.close()
			
			self.sqlite_conn = None
			
			
	def create_local_table(self):
		pass


start_time = datetime.now()
print('starting import at: \t\t{}'.format(start_time))
app = App()
end_time = datetime.now()
print('finished import at: \t\t{}'.format(end_time))
print('total import time: \t\t{}'.format(end_time - start_time))
