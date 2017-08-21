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
	try:
		with pyodbc.connect(driver='{ODBC Driver 13 for SQL Server}',host='databaseservername',database='databasename',UID=user,PWD=pw) as conn:
			cur = conn.cursor()
			#print('All is Well. Returning Cursor object')
			return cur
	except pyodbc.Error as e:
		print(e[1])


def get_tables(in_cursor,database='databasename',schema='dbo'):
	"""
	Description: Returns list of tables in a database.
	Parameters: cursor object, database name and schema name
	Returns: List of all table names
	"""
	try:
		table_list = []
		for row in in_cursor.tables(catalog=database,schema=schema):
			table_list.append(row.table_name)
			#table_list.append('[{}].[{}]'.format(row.table_schem, row.table_name))
			#print(row.table_schem, row.table_name)
		return table_list
	except pyodbc.Error as e:
		print(e[1])


def get_spatial_tables(in_cursor, schema='dbo'):
	"""
	Description: This table returns list of all spatial tables.
	Parameters, accepts two arguments cursor and schema name
	Returns: List of table
	"""	
	in_cursor.execute("""select c.TABLE_NAME from information_schema.columns c join 
        information_schema.tables t ON c.TABLE_NAME = t.TABLE_NAME
        AND t.TABLE_TYPE = 'BASE TABLE' where c.DATA_TYPE = 'geometry'
         and c.TABLE_SCHEMA=?""",schema)
	spatial_tables = [s[0] for s in in_cursor.fetchall()]
	return spatial_tables


def check_primarykey(in_cur, in_table):
	"""
	Description: This function checks if table has primary key and returns boolean
	Parameters: cursor object and table
	Returns: Boolean
	"""
	try:
		for r in in_cur.primaryKeys(table=in_table):
			if r.pk_name:
				return True
			else:
				return False
	except pyodbc.Error as e:
		print(e[1])


def check_spatialindex(in_cur, in_table):
	"""
	Description: This function checks if table has spatial index
	Parameters: cursor and table
	Returns: Boolean
	"""
	try:
		in_cur.execute("""select * from sys.indexes 
							where object_id = (select object_id from sys.objects where name = ?) 
							and type_desc='SPATIAL'""",in_table)
		r = [v[1] for v in in_cur.fetchall()]
		#print(r)
		if not r:
			return False
		else:
			return True
	except pyodbc.Error as e:
		print(e[1])


def get_boundingbox(in_cur, in_table):
	"""
	Description: This function returns dictionary of bounding box coordinates. 
	Parameters: cursor and table
	Returns: Dictionary of bounding box coordinates
	"""
	try:

		key_list = ['MinX','MinY','MaxX','MaxY']
		in_cur.execute("""SELECT
			geometry::EnvelopeAggregate(GEOM).STPointN(1).STX AS MinX,
			geometry::EnvelopeAggregate(GEOM).STPointN(1).STY AS MinY,
			geometry::EnvelopeAggregate(GEOM).STPointN(3).STX AS MaxX,
			geometry::EnvelopeAggregate(GEOM).STPointN(3).STY AS MaxY
			FROM {}""".format(in_table))
		coords = cur.fetchall()
		value_list = [coord for coord in coords[0]]
		return dict(zip(key_list,value_list))
	except pyodbc.Error:
		print('Bounding box for {} could not be calculated'.format(in_table))

def create_spatialindex(in_cur, in_table):
	"""
	Description: This function creates spatial index on a table. 
	Parameters: Accepts two arguments cursor and table.
	Doesn't return any object. Remember to commit if autocommit is not set in the connection object. 
	"""
	try:
		bb=get_boundingbox(in_cur, in_table)
		#print(bb)
		print('Creating Spatial Index for {}'.format(in_table))
		in_cur.execute("""CREATE SPATIAL INDEX SIDX_{} ON {}(GEOM)
						USING GEOMETRY_GRID WITH(
						BOUNDING_BOX=(xmin={},ymin={},xmax={},ymax={}),
						GRIDS=(MEDIUM, MEDIUM, MEDIUM, MEDIUM),
						CELLS_PER_OBJECT=16) """.format(in_table,in_table,bb['MinX'],bb['MinY'],bb['MaxX'],bb['MaxY']))
		in_cur.commit()
		print('Spatial Index for {} created successfully.'.format(in_table))
	except pyodbc.Error as e:
		print(e[1])


cur = create_cursor()

tables = get_spatial_tables(cur)

for t in sorted(tables):
	check_si = check_spatialindex(cur, t)
	#print(check_si)
	try:
		if not check_si:
			create_spatialindex(cur, t)
		else:
			print('Table {} has spatial index.'.format(t))
	except pyodbc.Error as e:
		print(e[1])

