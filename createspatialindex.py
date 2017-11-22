##################################
# Description: Python script to get spatial tables, check for spatial index and creating spatial index
##################################

# Importing the required modules
import pyodbc

def create_cursor():
	"""
	Description: This function returns a cursor object.
	This function accepts no arguments.
	Return: Cursor object
	"""
	user='username'
	pw='password'
	with pyodbc.connect(driver='{ODBC Driver 13 for SQL Server}',host='databaseservername',database='databasename',UID=user,PWD=pw) as conn:
		cur = conn.cursor()
		#print('All is Well. Returning Cursor object')
		if cur:
			return cur

'''		
def get_tables(in_cursor,database='databasename',schema='dbo'):
	"""
	Description: Returns list of tables in a database.
	Parameters: cursor object, database name and schema name
	Returns: List of all table names
	"""
	table_list = []
	for row in in_cursor.tables(catalog=database,schema=schema):
		table_list.append(row.table_name)
	if table_list:
		return table_list
'''

def get_spatial_tables(in_cursor, schema='dbo'):
	"""
	Description: This table returns list of all spatial tables (geometry).
	Parameters, accepts two arguments cursor and schema name
	Returns: List of table
	"""	
	in_cursor.execute("""select c.TABLE_NAME from information_schema.columns c join 
        information_schema.tables t ON c.TABLE_NAME = t.TABLE_NAME
        AND t.TABLE_TYPE = 'BASE TABLE' where c.DATA_TYPE = 'geometry'
         and c.TABLE_SCHEMA=?""",schema)
	spatial_tables = [s[0] for s in in_cursor.fetchall()]
	if spatial_tables:
		return spatial_tables


def has_spatialindex(in_cur, in_table):
	"""
	Description: This function checks if table has spatial index
	Parameters: cursor and table
	Returns: Boolean
	"""
	in_cur.execute("""select * from sys.indexes 
			where object_id = (select object_id from sys.objects where name = ?) 
			and type_desc='SPATIAL'""",in_table)
	
	r = [v[1] for v in in_cur.fetchall()] # Getting the table names only
	#print(r)
	if not r:
		return False
	else:
		return True

def has_primarykey(in_cur, in_table):
	"""
	Description: This function checks if table has primary key and returns boolean
	Parameters: cursor object and table
	Returns: True if table has primary key
	"""
	for r in in_cur.primaryKeys(table=in_table):
		if r.pk_name:
			return True
		else:
			return False

	
def get_boundingbox(in_cur, in_table):
	"""
	Description: This function returns dictionary of bounding box coordinates. 
	Parameters: cursor and table
	Returns: Dictionary of bounding box coordinates
	"""
	key_list = ['MinX','MinY','MaxX','MaxY']
	in_cur.execute("""SELECT
		geometry::EnvelopeAggregate(GEOM).STPointN(1).STX AS MinX,
		geometry::EnvelopeAggregate(GEOM).STPointN(1).STY AS MinY,
		geometry::EnvelopeAggregate(GEOM).STPointN(3).STX AS MaxX,
		geometry::EnvelopeAggregate(GEOM).STPointN(3).STY AS MaxY
		FROM {}""".format(in_table))
	coords = cur.fetchall()
	value_list = [coord for coord in coords[0]]
	if value_list:
		return dict(zip(key_list,value_list))


def create_spatialindex(in_cur, in_table,in_boundingbox):
	"""
	Description: This function creates spatial index on a table. 
	Parameters: Accepts three arguments cursor, table and bounding box as dict.
	Doesn't return any object. Remember to commit if autocommit is not set in the connection object. 
	"""
	print('Creating Spatial Index for {}'.format(in_table))
	in_cur.execute("""CREATE SPATIAL INDEX SIDX_{} ON {}(GEOM)
					USING GEOMETRY_GRID WITH(
					BOUNDING_BOX=(xmin={},ymin={},xmax={},ymax={}),
					GRIDS=(MEDIUM, MEDIUM, MEDIUM, MEDIUM),
					CELLS_PER_OBJECT=16) """.format(in_table,in_table,in_boundingbox['MinX'],in_boundingbox['MinY'],in_boundingbox['MaxX'],in_boundingbox['MaxY']))
	in_cur.commit()
	print('Spatial Index for {} created successfully.'.format(in_table))

def set_primarykey(in_cur, in_table):
	"""
	This function takes cursor and table as argument and sets the primary key for the table.
	Assumes that the table has column named OBJECTID on which primary key will be set.
	"""
	print('Setting Primary Key for {}'.format(in_table))
	in_cur.execute(""" 
		ALTER TABLE {0} ADD CONSTRAINT PK_{1}_OBJECTID PRIMARY KEY CLUSTERED (OBJECTID)
		""".format(in_table, in_table))
	in_cur.commit()
	print('Primary key PK_{}_OBJECTID set for table {}'.format(in_table,in_table))
	

try:
	cur = create_cursor()
	tables = get_spatial_tables(cur)

	# Get all tables without spatial index
	tbls_without_si =[t for t in tables if not has_spatialindex(cur,t)]

	# Get all tables without primary key
	tbls_without_pk = [t for t in tbls_without_si if not has_primarykey(cur, t)]

	if tbls_without_si:
		for t in tbls_without_si:
			if t in tbls_without_pk:
				set_primarykey(cur, t)
				create_spatialindex(cur,t,get_boundingbox(cur,t))
			else:
				create_spatialindex(cur,t,get_boundingbox(cur,t))
	else:
		print('All tables have spatial index')

except Exception as e:
	print(e)

