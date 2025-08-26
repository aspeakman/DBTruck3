"""
Copyright (C) 2025  Andrew Speakman, parts also copyright (C) 2012 ScraperWiki Ltd. and other contributors

This file is part of DBTruck3, a relaxed schema-less interface to common databases with a DB API2.0 PEP-0249  interface

DBTruck3 is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

DBTruck3 is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
from urllib.parse import urlsplit
from collections import OrderedDict
import re

from .convert import nquote, simplify, iquote
from . import adapcast
from .adapcast import Pickle, ISODate, ISODateTime, ISODateTTime, postcast_text as text

class Store(object):

    # A class to control a connection to an SQLite, MySQL, Postgres or SQL Server database - based on the old ScraperWiki dumptruck module

    """Top level method types as follows:
    DDL (data definition) = create, drop, create index, drop index - no rollback as commit is included (already implicit in mySQL/SQLite)
    DML (data modification) = execute, save, delete, insert, save_var, clear_vars - changes committed on exit unless commit=False
    INF (data selection) = select, dump, count, get_max, get_min, select_list, match_list, get_var, all_vars, column_info, tables, columns, key_columns, indices - read only
    """

    _poss_db_types = [ 'SQLITE', 'POSTGRESQL', 'MYSQL', 'SQLSERVER' ] # just an aide memoire not used
  
    def __init__(self, connect_details, data_table = 'dbtruckdata', vars_table = 'dbtruckvars', default_commit = True, 
            timeout = 5, json_str_output = False, dates_str_output = False, bool_int_output = False, sqlite_t_sep = False,
            has_rowids = False, text_key_width = 100):
                
        # note that sqlite_t_sep just specifies the internal storage format for SQLite datetimes
        # the string output of dates is always ISO8601 format (T separator)
        
        # note that text_key_width only sets the fixed size of text fields in MySQL and SQL Server databases used as keys or in indexes
                        
        _tables = [] # current sorted list of tables in the store
            
        # Note default_commit True by default - means there will be a commit UNLESS commit=False 
        if not isinstance(default_commit, bool):
            raise TypeError('default_commit must be True or False.')
        else:
            self._default_commit = default_commit
        
        # Make sure it's a good table name
        if not isinstance(data_table, str):
            raise TypeError('data_table must be a string')
        else:
            self._data_table = data_table
            
        # Make sure it's null or a good table name
        if vars_table and not isinstance(vars_table, str):
            raise TypeError('vars_table must be a string')
        else:
            self._vars_table = vars_table
        
        if not connect_details:
            raise RuntimeError("No database connection details supplied")
            
        self._has_rowids = has_rowids
        self._text_key_width = text_key_width
        
        self._qchar = '"' # default quote character for identifiers
        self.db_type = Store.type_from_uri(connect_details).upper()
        if self.db_type == 'POSTGRESQL':
            if not hasattr(adapcast, 'POSTGRES_CREATE_SEQ'):
                raise ImportError("The 'psycopg2' package is not installed")
            self._create_sequence = adapcast.POSTGRES_CREATE_SEQ
            self._adapt_map = adapcast.POSTGRES_ADAPTER_MAP
            self._convert_map = dict(adapcast.POSTGRES_CONVERTER_MAP)
            self._cast_map = dict(adapcast.POSTGRES_CAST_MAP)
            self._convert_alt = dict(adapcast.POSTGRES_CONVERT_ALT)
            self._dbmodule = __import__('psycopg2')
            connect_kwargs = {
                'connect_timeout': timeout,
            }
            self.connection = self._dbmodule.connect(connect_details, **connect_kwargs)
            self.connection.autocommit = False
        elif self.db_type == 'MYSQL':
            self._qchar = '`' # uses backticks as quote characters for identifiers
            if not hasattr(adapcast, 'MYSQL_CREATE_SEQ'):
                raise ImportError("The 'mysql.connector' package is not installed")
            self._create_sequence = adapcast.MYSQL_CREATE_SEQ
            self._cast_map = dict(adapcast.MYSQL_CAST_MAP)
            self._dbmodule = __import__('mysql.connector', fromlist=['connector'])
            connect_kwargs = {
                'connection_timeout': timeout,
                'converter_class': adapcast.DBTruckMySQLConverter,
            }
            u = urlsplit(connect_details)
            if u.username: connect_kwargs['user'] = u.username
            if u.password: connect_kwargs['password'] = u.password
            if u.port: connect_kwargs['port'] = u.port
            if u.hostname: connect_kwargs['host'] = u.hostname
            database = str(u[2]).replace('/', '')
            if database: connect_kwargs['database'] = database
            self.connection = self._dbmodule.connect(**connect_kwargs)
            self.connection.autocommit = False  
        elif self.db_type == 'SQLSERVER':
            if not hasattr(adapcast, 'SQLSERVER_CREATE_SEQ'):
                raise ImportError("The 'mssql_python' package is not installed")
            self._create_sequence = adapcast.SQLSERVER_CREATE_SEQ
            self._cast_map = dict(adapcast.SQLSERVER_CAST_MAP)
            self._dbmodule = __import__('mssql_python')
            self.connection = self._dbmodule.connect(connect_details)
            self.connection.setautocommit(False)
        else:
            self.db_type = 'SQLITE'
            self._sqlite_t_sep = sqlite_t_sep
            if self._sqlite_t_sep: # non-standard separator for date times on SQLite can be a 'T'
                self._create_sequence = adapcast.SQLITE_T_CREATE_SEQ
                self._adapt_map = adapcast.SQLITE_T_ADAPTER_MAP
            else:
                self._create_sequence = adapcast.SQLITE_CREATE_SEQ
                self._adapt_map = adapcast.SQLITE_ADAPTER_MAP
            self._convert_map = adapcast.SQLITE_CONVERTER_MAP
            self._cast_map = {} # not used
            self._convert_alt = adapcast.SQLITE_CONVERT_ALT
            self._dbmodule = __import__('sqlite3') # note autocommit mode is off by default
            connect_kwargs = {
                'detect_types': self._dbmodule.PARSE_DECLTYPES|self._dbmodule.PARSE_COLNAMES, 
                'timeout': timeout
            }
            self.connection = self._dbmodule.connect(connect_details, **connect_kwargs)
        if not self.connection:
            raise RuntimeError("Could not connect to database '%s'" % connect_details)
        if self.db_type == 'MYSQL':
            self.cursor = self.connection.cursor(buffered=True)
        elif self.db_type == 'SQLSERVER':
            if self.connection._closed:
                raise mssql_python.exceptions.InterfaceError(
                    driver_error="Cannot create cursor on closed connection",
                    ddbc_error="Cannot create cursor on closed connection",
                )
            self.cursor = adapcast.DBTruckSQLServCursor(self.connection)
            self.connection._cursors.add(self.cursor)  # Track the cursor
        else:
            self.cursor = self.connection.cursor()
        
        self._phchar = '%s' if self._dbmodule.paramstyle == 'pyformat' else '?' # place holder marker in prepared statements
        
        self._json_str_output = json_str_output
        self._dates_str_output = dates_str_output
        self._bool_int_output = bool_int_output
            
        if self.db_type == 'MYSQL':
            self.connection.converter._json_str_output = self._json_str_output
            self.connection.converter._dates_str_output = self._dates_str_output
            self.connection.converter._bool_int_output = self._bool_int_output
        elif self.db_type == 'SQLSERVER':
            if self._bool_int_output is False:
                self._cast_map.pop('boolint', None)
            if self._dates_str_output is False:
                self._cast_map.pop('isoformat', None)
            if self._json_str_output is True:
                self._cast_map.pop('json', None)
                self._cast_map.pop('jsontuple', None)
                self._cast_map.pop('jsonset', None)
        else:
            if self.db_type == 'SQLITE': # first remove two default converters in SQLite
                self._dbmodule.register_converter('date', adapcast.convert_clear)
                self._dbmodule.register_converter('timestamp', adapcast.convert_clear)
            self._register_adapters()
            self._register_converters(alt=True)
            
        self._check_or_create_vars_table() # includes an implicit commit
        
        self.tables() # stores fresh _tables list
        
    def __del__(self):
        if getattr(self, 'connection', None):
            try:
                self.connection.close()
            except:
                pass
                
    @staticmethod   
    def type_from_uri(db_uri):
        db = 'SQLite'
        if db_uri.startswith('postgresql://') or db_uri.startswith('postgres://'):
            db = 'PostgreSQL'
        elif db_uri.startswith('mysql://'):
            db = 'MySQL'
        elif ';' in db_uri:
            db = 'SQLServer'
        return db
        
    def commit(self, implicit = False):
        'Commit database transactions.'
        #if self.db_type == 'SQLITE' and implicit: # skips wastefule explicit CREATE/ALTER/DROP TABLE commits for SQLite and mySQL
        #    return
        self.connection.commit()
    
    def rollback(self):
        'Rollback database transactions.'
        self.connection.rollback()

    def close(self):
        self.connection.close()

    def create_table(self, data, table_name=None, keys = [], error_if_exists = False): 
        'Create a table based on the data, but dont insert anything.'
        table_name = table_name if table_name else self._data_table
        
        this_data = self._clean_data(data) # Turns it into a list of lists of (key, value) tuples (None values removed)
        if len(this_data) == 0 or len(this_data[0]) == 0:
            raise ValueError('No data sample values, or all the values were null.')
            
        if self.db_type == 'SQLSERVER':
            if_not_exists = '' if error_if_exists else 'IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = %s) CREATE TABLE' % self._phchar
        else:
            if_not_exists = '' if error_if_exists else 'CREATE TABLE IF NOT EXISTS'
                        
        if keys:
        
            startdata = OrderedDict(this_data[0]) # firsr row
            # Check all keys are in the row and are not null
            keycoldefs = {}
            for k, v in startdata.items():
                if k in keys:
                    if v is None:
                        raise ValueError('A key in the first sample data row was empty.')
                    else:
                        col_type = self._obj_column_type(v)
                        col_type = self._index_col_type(col_type)
                        keycoldefs[k] = self.iquote(k) + ' ' + col_type
            for k in keys:
                if k not in keycoldefs:
                    raise ValueError('A key was missing from the first sample data row.')
                    
            pkeys =  ', '.join( [ self.iquote(k) for k in keys ] )
            coldefs = ', '.join( [ keycoldefs[k] for k in keys ] )
            if self._has_rowids:
                if self.db_type == 'POSTGRESQL': # need an explicit 'rowid' field
                    coldefs = coldefs + ', rowid bigserial unique'
                elif self.db_type == 'MYSQL': # need an explicit 'rowid' field
                    coldefs = coldefs + ', rowid bigint auto_increment unique'
                elif self.db_type == 'SQLSERVER': # need an explicit 'rowid' field
                    coldefs = coldefs + ', rowid bigint identity(1,1)'
            sqlsubs = (if_not_exists, self.iquote(table_name), coldefs, pkeys)
            sql = '%s %s (%s, PRIMARY KEY (%s));' # create table with keys only
        
        else:
        
            startdata = OrderedDict(this_data[0]) # firsr row
            # Select the first non-null item from the row
            v = None
            for k, v in startdata.items():
                if v is not None:
                    break
            if v is None: 
                raise ValueError('All the values in the first sample data row were null.')
            
            sqlsubs = (if_not_exists, self.iquote(table_name), self.iquote(k), self._obj_column_type(startdata[k]) ) 
            sql = '%s %s (%s %s' # create table with one unkeyed column
            if self._has_rowids:
                if self.db_type == 'POSTGRESQL': # need an explcit 'rowid' field
                    sql = sql + ', rowid bigserial unique);'
                elif self.db_type == 'MYSQL': # need an explicit 'rowid' field
                    sql = sql + ', rowid bigint auto_increment unique);'
                elif self.db_type == 'SQLSERVER': # need an explicit 'rowid' field
                    sql = sql + ', rowid bigint identity(1,1));'
                else:
                    sql = sql + ');'
            else:
                sql = sql + ');'
        
        #print (sql % sqlsubs)
        if self.db_type == 'SQLSERVER':
            self._execute_norows(sql % sqlsubs, [table_name])
        else:
            self._execute_norows(sql % sqlsubs) 
         
        for row in this_data:
            self._check_and_add_columns(table_name, row) # update with any other columns 
            
        self.commit(implicit = True)
        self.tables() # stores fresh _tables list
                    
    def create(self, *args, **kwargs):
        self.create_table(*args, **kwargs)
                
    def drop_table(self, table_name, if_exists = False): # table has to be named
        if self.db_type == 'SQLSERVER' and if_exists:
            sql = """IF EXISTS ( SELECT * FROM information_schema.tables WHERE table_name = %s )
                DROP TABLE %s;""" % (self._phchar, self.iquote(table_name))
            self._execute_norows(sql, [table_name])
        else:
            sql = 'DROP TABLE%s %s;' % (' IF EXISTS' if if_exists else '', self.iquote(table_name))
            self._execute_norows(sql)
        self.commit(implicit = True)
        self.tables() # stores fresh _tables list
        
    def drop(self, *args, **kwargs):
        self.drop_table(*args, **kwargs)
        
    def _col_type(self, storage_type, dc_type): # unquoted for use in matching
        if self.db_type == 'SQLITE':
            return dc_type + ' ' + storage_type if dc_type else storage_type
        else:
            return storage_type # in PostGres/mySQL the column type is the storage type
        
    def _real_ctype(self, storage_type, dc_type): # full column type quoted if necessary 
        if self.db_type == 'SQLITE':
            col_type = dc_type + ' ' + storage_type if dc_type else storage_type
            return self.iquote(col_type, force=True) # in SQLite column types force quotes because they can contain spaces
        elif self.db_type == 'MYSQL' and storage_type in [ 'varchar', 'varbinary' ] :
            return storage_type + '(%d)' % self._text_key_width # in MySQL set a default max size for specified types
        elif self.db_type == 'MYSQL' and storage_type in [ 'time', 'datetime', 'timestamp' ] :
            return storage_type + '(6)' # in MySQL allow for microseconds in date times/timestamps
        else:
            return storage_type # in PostGres/MySQL the column type is the storage type (unquoted keyword)
            
    def _obj_column_type(self, obj): # note results for use in SQL COLUMN ADD statements can be quoted with spaces OR unquoted without spaces
        'Decide the type of a column to contain an object.'
        type_match, storage_type, dc_type = self._create_col_match(obj)
        if type_match is None:
            raise ValueError ('Objects of type %s cannot be stored' % str(type(obj)))
        return self._real_ctype(storage_type, dc_type)
        
    def _obj_column_cast(self, obj): # note results for use in SQL COMMENT statements
        'Decide if a column needs to be cast to a different type after database extraction.'
        if self.db_type == 'SQLITE':
            return None
        type_match, storage_type, dc_type = self._create_col_match(obj)
        return dc_type # can be None
    
    def _obj_type_label(self, obj): 
        'Return the type label = class name string OR wrap type of the adapter class.'
        type_match, storage_type, dc_type = self._create_col_match(obj)
        if type_match is None:
            return None
        elif 'cast' in dir(type_match):
            return type_match._wrap_name 
        else:
            return type_match.__name__
        
    def _obj_for_adapting(self, obj): 
        'Return the object to adapt directly OR indirectly by being wrapped in an adapter class.'
        type_match, storage_type, dc_type = self._create_col_match(obj)
        if type_match is None:
            return None
        elif 'cast' in dir(type_match):
            # return obj if it is already an Adapter instance otherwise wrap it in the appropriate Adapter type
            return obj if isinstance(obj, type_match) else type_match(obj)
        else:
            return obj
        
    def _create_col_match(self, obj): # obj should be an object recognised by an Adapter or a basic Python type
        for type_match, storage_type, dc_type in self._create_sequence:
            if type_match is None: # skip any null entries
                continue
            if 'cast' in dir(type_match): # attribute flags an Adapter class
                if isinstance(obj, type_match) or type_match.cast(obj):
                    # EITHER the object is already wrapped in an Adapter instance OR the Adapter 'cast' static method recognises the object
                    return type_match, storage_type, dc_type
            else:
                if type_match == type(obj): # do types match
                    return type_match, storage_type, dc_type
        return None, None, None
            
    def column_info(self, table_name=None): 
        ' dictionary of all columns keyed by name with the column type as the value'
        table_name = table_name if table_name else self._data_table
        cols = {}
        if self.db_type == 'POSTGRESQL':
            sql = """SELECT column_name, data_type FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = %s;""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            for row in rows:
                cols[text(row[0])] = text(row[1])  
        elif self.db_type == 'MYSQL' or self.db_type == 'SQLSERVER':
            sql = """SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns WHERE table_name = %s;""" % self._phchar
            rows = self._execute_rows(sql, [table_name])  
            for row in rows:
                if row[2] is None or row[2] >= 65535:
                    size = ''
                elif row[2] <= 0:
                    size = '(max)'
                else:
                    size = '(%d)' % row[2]
                cols[text(row[0])] = text(row[1]) + size
        else:
            sql = 'PRAGMA table_info(%s)' % self.iquote(table_name)
            rows = self._execute_rows(sql)
            for row in rows:
                cols[text(row[1])] = text(row[2]).split()[0] # value is derived column type (before first space - see SQLite)
            if self._has_rowids:
                cols[u'rowid'] = u'sequence integer'
        return cols 
    
    def columns(self, table_name=None):
        ' alphabetically ordered list of column names '
        table_name = table_name if table_name else self._data_table
        return sorted(self.column_info(table_name).keys())
        
    def _column_comments(self, table_name=None):
        ' dictionary of column comments keyed by column name '
        table_name = table_name if table_name else self._data_table
        cols = {}
        if self.db_type == 'POSTGRESQL':
            sql = """SELECT column_name, col_description('public.%s'::regclass, ordinal_position) AS comment
                FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;""" % (table_name, self._phchar)
            rows = self._execute_rows(sql, [table_name]) 
            for row in rows:
                cols[text(row[0])] = text(row[1]) 
        elif self.db_type == 'MYSQL':
            sql = """SELECT column_name, column_comment
                FROM information_schema.columns WHERE table_name = %s;""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            for row in rows:
                cols[text(row[0])] = text(row[1])
        elif self.db_type == 'SQLSERVER':
            sql = """SELECT c.name as column_name, cast(ep.value as nvarchar) as column_comment
                FROM sys.tables AS t INNER JOIN sys.columns AS c ON t.object_id = c.object_id
                LEFT JOIN sys.extended_properties AS ep ON ep.major_id = c.object_id 
                AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
                WHERE t.name = %s;""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            for row in rows:
                cols[text(row[0])] = text(row[1])
        return cols 
        
    def tables(self):
        ' alphabetically ordered list of table names '
        if self.db_type == 'POSTGRESQL':
            sql = """SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY name;"""
        elif self.db_type == 'SQLSERVER':
            sql = """SELECT table_name AS name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ORDER BY name;"""
        elif self.db_type == 'MYSQL':
            sql = """SELECT table_name AS name FROM information_schema.tables ORDER BY name;"""
        else:
            sql = """SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"""
        rows = self._execute_rows(sql) 
        self._tables = sorted([ text(row[0]) for row in rows ])
        return self._tables
        
    def key_columns(self, table_name=None): 
        ' list of columns that are primary keys (note: order of names in the result list has no significance) '
        table_name = table_name if table_name else self._data_table
        if self.db_type == 'POSTGRESQL':
            sql = """select kcu.column_name as key_column,
                       kcu.ordinal_position as position
                from information_schema.table_constraints tco
                join information_schema.key_column_usage kcu 
                     on kcu.constraint_name = tco.constraint_name
                     and kcu.constraint_schema = tco.constraint_schema
                     and kcu.constraint_name = tco.constraint_name
                where tco.constraint_type = 'PRIMARY KEY'
                     and kcu.table_schema = 'public'
                     and kcu.table_name = %s
                order by position;""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            return [ text(row[0]) for row in rows ]
        elif self.db_type == 'MYSQL':
            sql = """SELECT column_name
                FROM information_schema.columns WHERE table_name = %s AND column_key = 'PRI';""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            return [ text(row[0]) for row in rows ]
        elif self.db_type == 'SQLSERVER':
            sql = """SELECT column_name
                FROM information_schema.key_column_usage WHERE 
                OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                AND table_name = %s;""" % self._phchar
            rows = self._execute_rows(sql, [table_name]) 
            return [ text(row[0]) for row in rows ]
        else:
            sql = 'PRAGMA table_info(%s)' % self.iquote(table_name)
            rows = self._execute_rows(sql) 
            return [ text(row[1]) for row in rows if row[5] != 0 ]
            
    def _check_and_add_columns(self, table_name, data_row): 
        columns = list(self.column_info(table_name).keys())
        # first check for explicit rowid column if required
        if self._has_rowids:
            if self.db_type == 'POSTGRESQL' and 'rowid' not in columns:
                    sql = 'ALTER TABLE %s ADD COLUMN rowid bigserial unique;'
                    self._execute_norows(sql % self.iquote(table_name)) 
            elif self.db_type == 'MYSQL' and 'rowid' not in columns:
                    sql = 'ALTER TABLE %s ADD COLUMN rowid bigint auto_increment unique;'
                    self._execute_norows(sql % self.iquote(table_name)) 
            elif self.db_type == 'SQLSERVER' and 'rowid' not in columns:
                    sql = 'ALTER TABLE %s ADD rowid bigint identity(1,1);'
                    self._execute_norows(sql % self.iquote(table_name)) 
        for key, value in data_row:
            if key not in columns and value is not None:
                column_type = self._obj_column_type(value)
                sqlsubs = (self.iquote(table_name), self.iquote(key), column_type) 
                if self.db_type == 'SQLSERVER':
                    sql = 'ALTER TABLE %s ADD %s %s;'
                else:
                    sql = 'ALTER TABLE %s ADD COLUMN %s %s;' 
                self._execute_norows(sql % sqlsubs) 
            if self.db_type == 'POSTGRESQL': # use the Postgres comment field to remove/store any post extraction cast information
                column_cast = self._obj_column_cast(value)
                if column_cast:
                    sql = 'COMMENT ON COLUMN %s.%s IS %s;' % (self.iquote(table_name), self.iquote(key), self._phchar)
                    self._execute_norows(sql, [column_cast]) # comment removed if None
            elif self.db_type == 'MYSQL': # use the mySQL comment field to remove/store any post extraction cast information
                column_cast = self._obj_column_cast(value)
                if column_cast:
                    sqlsubs = (self.iquote(table_name), self.iquote(key), column_type, nquote(column_cast)) 
                    sql = 'ALTER TABLE %s MODIFY COLUMN %s %s COMMENT %s;'
                    self._execute_norows(sql % sqlsubs)
            elif self.db_type == 'SQLSERVER': # use the extended property description field to remove/store any post extraction cast information
                column_cast = self._obj_column_cast(value)
                if column_cast:
                    sqlsubs = (nquote(column_cast), nquote(table_name), nquote(key)) 
                    sql = """EXEC sp_addextendedproperty 
                        @name = 'MS_Description', @value = %s,
                        @level0type = 'Schema', @level0name = 'dbo',
                        @level1type = 'Table', @level1name = %s, 
                        @level2type = 'Column', @level2name = %s;"""
                    self._execute_norows(sql % sqlsubs)
                
    def execute(self, sql, *args, **kwargs): 
        """ note executes a raw SQL statement - so must be quoted where necessary 
        and use appropriate place holders for the target DB """
        
        rows = self._execute_rows(sql, *args)

        self._commit_if_default(kwargs)

        if not self.cursor.description: # a list of tuples (first item is column name, aliased if necessary)
            return None
        else:
            colnames = [ text(d[0]) for d in self.cursor.description ] 
            rawdata = [ OrderedDict(zip(colnames, row)) for row in rows ]
            return rawdata
            
    def _execute_norows(self, sql, *args):
        ' execute an SQL statement returning no data (apart from row count) and no commit either '
        #print(sql, args)
        self.cursor.execute(sql, *args)
        if self.cursor.rowcount >= 0:
            return self.cursor.rowcount
        return None
        
    def _execute_rows(self, sql, *args): 
        ' execute an SQl statement and return any result (no commit) '
        self.cursor.execute(sql, *args)
        try:
            return self.cursor.fetchall() # result is a list of tuples (or empty list)
        except self._dbmodule.ProgrammingError: # Postgres throws this if the fetchall() command produces no results 
            return []
            
    """def execute_query(self, sqlquery, data=[], **kwargs): no longer requuired
        if data is None:
            data = []
        elif not isinstance(data, (list, tuple)):
            data = [data] # Allow for a non-list to be passed as data.
        result = self.execute(sqlquery, data, **kwargs)
        if not result: # None (non-select) and empty list (select) results
            return { 'data': [], 'keys': [] }
        return { 'data': list(map(lambda row: list(row.values()), result)),
                 'keys': list(result[0].keys())
                }"""
            
    def _sql_ph_check(self, sql):
        # SQL place holders/parameters assumed to be qmark (?) format - but Postgres and MySQL use pyformat (%s)
        # note simple replace below might get confused if there are quoted ?/%s in the query?
        if self._dbmodule.paramstyle == 'pyformat':
            return sql.replace('%s', '%%s').replace('?', '%s')
        else:
            return sql
            
    def _commit_if_default(self, kwargs):
        if kwargs.get('commit', self._default_commit):
            self.connection.commit()   
    
    def dump(self, **kwargs): # alias for select
        return self.select(**kwargs)
        
    def vacuum(self): 
        if self.db_type == 'MYSQL':
            tlist = self._tables
            for t in tlist:
                self._execute_norows('OPTIMIZE TABLE %s;' % self.iquote(t))
        elif self.db_type == 'POSTGRESQL':
            self.connection.autocommit = True
            self._execute_norows('VACUUM;')
            self.connection.autocommit = False
        elif self.db_type != 'SQLSERVER':
            self._execute_norows('VACUUM;') 
        
    def lock(self): # implicit commit
        if self.db_type != 'SQLITE': return False
        try:
            self._execute_norows("PRAGMA LOCKING_MODE = EXCLUSIVE") 
            # This locks the database for writing AND reading - no other access allowed
            # NB the lock is only fully exclusive after first write, it is released when the db is disconnected
            return True
        except self._dbmodule.Error as e:
            return False

    def unlock(self): # implicit commit
        if self.db_type == 'SQLITE': return False
        try:
            self._execute_norows('PRAGMA LOCKING_MODE = NORMAL') 
            return True
        except self._dbmodule.Error:
            return False
            
    def select(self, fields=None, table_name=None, conditions=None, params=[]):
        """ retrieve data from a datastore table where conditions apply, result is a list of dicts
        fields can be None - returns all fields (including rowid) 
                      string - expression supplied directly to an SQL SELECT statement
                      tuple - simple (field, alias) format
                      list of fields - specifies multiple fields without aliases 
                      dict of fields (with aliases as values) (can be an OrderedDict)
        output is always a list of OrderedDicts"""
        table_name = table_name if table_name else self._data_table
        target = None
        if not fields: # catches empty lists and dicts
            target = '*, rowid' if self.db_type == 'SQLITE' and self._has_rowids else "*" # always include rowid in full list - implicit in Postgres/mySQL
        elif isinstance(fields, str): # simple string
            target = fields
        elif isinstance(fields, tuple) and len(fields) == 2 and isinstance(fields[0], str): # pair
            column_fields = [ fields ]
        elif isinstance(fields, dict): # dict
            column_fields = [ ( k, v ) for k, v in fields.items() ]
        elif isinstance(fields, list) and isinstance(fields[0], str): # list of strings
            column_fields = [ ( f, None ) for f in fields ]
        elif isinstance(fields, list) and isinstance(fields[0], (tuple, list)) and len(fields[0]) == 2 and isinstance(fields[0][0], str): # list of pairs
            column_fields = fields
        else:
            raise TypeError('Fields must be a string, a dict, or an iterable of pairs ')  
        rfield_column_map = {} # maps result output fields back to table column names (k == v if no alias)
        if not target:
            items = []
            for c in column_fields: # always a list of 2 tuples
                if c[1]:
                    rfield_column_map[c[1]] = c[0]
                else:
                    rfield_column_map[c[0]] = c[0]
                item = '%s AS %s' % (self.iquote(c[0]), self.iquote(c[1])) if c[1] else self.iquote(c[0])
                items.append(item)
            target = ', '.join(items)
        if isinstance(params, str):
            params = [ params ]
        if conditions:
            if params:
                conditions = self._sql_ph_check(conditions)
            result = self.execute('SELECT %s FROM %s WHERE %s;' % (target, self.iquote(table_name), conditions), params, commit = False)
        else:
            result = self.execute('SELECT %s FROM %s;' % (target, self.iquote(table_name)), [], commit = False)
        #print('execute', result)
        if result and self._cast_map: # post extraction output casting based on stored column comments
            column_casttype_map = self._column_comments(table_name) # maps columns to cast type
            #print(column_casttype_map)
            if column_casttype_map:
                rowfield_castfunc_map = {} # result field to function map
                for rf in result[0].keys(): # iterate over result field names
                    col = rfield_column_map[rf] if rfield_column_map else rf # get real table column name
                    cast_type = column_casttype_map.get(col) # match to any cast type stored in the comments
                    if cast_type and self._cast_map.get(cast_type): # only add if there is a matching function?
                        rowfield_castfunc_map[rf] = self._cast_map[cast_type]
                if rowfield_castfunc_map:
                    for row in result:
                        for rowfield, castfunc in rowfield_castfunc_map.items():
                            row[rowfield] = castfunc(row[rowfield])
        return result
        
    def list_select(self, key_field, match_list, **kwargs):
        """ retrieve data from a datastore table where the key matches values in a list 
        note due to sqlite SQL variable limits the list should be under 999 in length """
        in_list = '%s IN (%s)' % (self.iquote(key_field), ','.join(['?' for m in match_list]))
        if kwargs.get('conditions'):
            kwargs['conditions'] = '(%s) AND %s' % (kwargs['conditions'], in_list)
        else:
            kwargs['conditions'] = in_list
        if kwargs.get('params'):
            if isinstance(kwargs['params'], str):
                kwargs['params'] = [ kwargs['params'] ]
            kwargs['params'].extend(match_list)
        else:
            kwargs['params'] = match_list
        return self.select(**kwargs)
        
    def match_select(self, match_dict, **kwargs):
        """ retrieve data from a datastore table where the match dict has fields which must match the target exactly"""
        equals_list = [ self.iquote(k)  + ' = ?' for k in match_dict.keys() ] # note keys iterate in same order as values() below
        if kwargs.get('conditions'):
            kwargs['conditions'] = '(%s) AND %s' % (kwargs['conditions'], ' AND '.join(equals_list))
        else:
            kwargs['conditions'] = ' AND '.join(equals_list)
        if kwargs.get('params'):
            if isinstance(kwargs['params'], str):
                kwargs['params'] = [ kwargs['params'] ]
            kwargs['params'].extend(match_dict.values()) # note guaranteed to iterate in same order as keys() above
        else:
            kwargs['params'] = list(match_dict.values()) # note guaranteed to iterate in same order as keys() above
        return self.select(**kwargs)

    def count(self, **kwargs):
        ' count the rows in the datastore table where conditions apply'
        kwargs['fields'] = 'COUNT(*) AS count'
        result = self.select(**kwargs)
        if not result or result[0].get('count') is None:
            return None
        return int(result[0]['count'])
        
    def get_max(self, field, **kwargs):
        ' get the max value for the field in datastore table where conditions apply'
        kwargs['fields'] = 'MAX(%s) AS maxval' % self.iquote(field)
        result = self.select(**kwargs)
        if not result or result[0].get('maxval') is None:
            return None
        return result[0]['maxval']
        
    def get_min(self, field, **kwargs):
        ' get the min value for the field in datastore table where conditions apply'
        kwargs['fields'] = 'MIN(%s) AS minval' % self.iquote(field)
        result = self.select(**kwargs)
        if not result or result[0].get('minval') is None:
            return None
        return result[0]['minval']
        
    def _var_type(self, val): 
        ' get a type name for use as a column name in the vars_table '
        vtype = None
        if isinstance(val, type):
            if '_wrap_name' in dir(val):
                vtype = val._wrap_name
            else:
                vtype = val.__name__
        else:
            vtype = val
        if vtype == 'long': 
            vtype = 'int'
        elif vtype == 'unicode':
            vtype = 'text'
        elif vtype == 'str':    
            vtype = 'text'
        return vtype 
                    
    def _check_or_create_vars_table(self): 
        if not self._vars_table:
            return
        # the vars table has one dc_type column for every possible data type
        if self.db_type == 'MYSQL':
            sql = 'CREATE TABLE IF NOT EXISTS %s (var_name varchar(%d) PRIMARY KEY, var_type varchar(%d));' \
                % (self.iquote(self._vars_table), self._text_key_width, self._text_key_width)
            self._execute_norows(sql)
        elif self.db_type == 'SQLSERVER':
            sql = """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = %s)
                CREATE TABLE %s (var_name varchar(%d) PRIMARY KEY, var_type varchar(%d));""" \
                % (self._phchar, self.iquote(self._vars_table), self._text_key_width, self._text_key_width)
            self._execute_norows(sql, [self._vars_table]) 
        else:
            sql = 'CREATE TABLE IF NOT EXISTS %s (var_name text PRIMARY KEY, var_type text);' % self.iquote(self._vars_table)
            self._execute_norows(sql) 
        columns = list(self.column_info(self._vars_table).keys())
        for type_match, storage_type, dc_type in self._create_sequence: # if sequence is changed need to run clear_vars()
            if type_match is None: # skip any null entries
                continue
            col_key = self._var_type(type_match)
            if col_key in columns: # already exists
                continue
            column_type = self._real_ctype(storage_type, dc_type) # note real column_type already quoted if required
            sqlsubs = (self.iquote(self._vars_table), self.iquote(col_key, force=True), column_type) # force quote because col_key can overlap with SQL keywords
            if self.db_type == 'SQLSERVER':
                sql = 'ALTER TABLE %s ADD %s %s;' % sqlsubs
            else:
                sql = 'ALTER TABLE %s ADD COLUMN %s %s;' % sqlsubs
            #print(sql)
            self._execute_norows(sql)
            columns.append(col_key)
            if dc_type:
                if self.db_type == 'POSTGRESQL': # use the Postgres comment field to store any post extraction cast
                    sql = 'COMMENT ON COLUMN %s.%s IS %s;' % (self.iquote(self._vars_table), self.iquote(col_key), self._phchar)
                    self._execute_norows(sql, [dc_type])
                elif self.db_type == 'MYSQL': # use the MySQL comment field to store any post extraction cast
                    sqlsubs = (self.iquote(self._vars_table), self.iquote(col_key, force=True), column_type, nquote(dc_type)) # force quote because col_key can overlap with SQL keywords
                    sql = 'ALTER TABLE %s MODIFY COLUMN %s %s COMMENT %s' % sqlsubs
                    self._execute_norows(sql)
                elif self.db_type == 'SQLSERVER': # use the extended property description field to store any post extraction cast
                    sqlsubs = (nquote(dc_type), nquote(self._vars_table), nquote(col_key))
                    sql = """EXEC sp_addextendedproperty 
                        @name = 'MS_Description', @value = %s,
                        @level0type = 'Schema', @level0name = 'dbo',
                        @level1type = 'Table', @level1name = %s, 
                        @level2type = 'Column', @level2name = %s;"""
                    self._execute_norows(sql % sqlsubs)
        self.commit(implicit = True)
            
    def get_var(self, name, default=None): 
        'Retrieve one saved variable from the database.'
        if not self._vars_table:
            return default
        if self.db_type == 'MYSQL':
            self.connection.converter._json_str_output = False
            self.connection.converter._dates_str_output = False
            self.connection.converter._bool_int_output = False
        elif self.db_type == 'SQLSERVER':
            self._cast_map = dict(adapcast.SQLSERVER_CAST_MAP)
            self._cast_map.pop('boolint', None)
            self._cast_map.pop('isoformat', None)
        else:
            #self._json_string_converters(False)
            #self._dates_string_converters(False) 
            self._register_converters(alt=False)
        data = self.select(table_name = self._vars_table, conditions = 'var_name = ?', params=name)
        if self.db_type == 'MYSQL':
            self.connection.converter._json_str_output = self._json_str_output
            self.connection.converter._dates_str_output = self._dates_str_output
            self.connection.converter._bool_int_output = self._bool_int_output
        elif self.db_type == 'SQLSERVER':
            self._cast_map = dict(adapcast.SQLSERVER_CAST_MAP)
            if self._bool_int_output is False:
                self._cast_map.pop('boolint', None)
            if self._dates_str_output is False:
                self._cast_map.pop('isoformat', None)
            if self._json_str_output is True:
                self._cast_map.pop('json', None)
                self._cast_map.pop('jsontuple', None)
                self._cast_map.pop('jsonset', None)
        else:
            #self._json_string_converters(self._json_str_output)
            #self._dates_string_converters(self._dates_str_output)
            self._register_converters(alt=True)
        if not data or len(data) != 1:
            return default
        else:
            value_col = data[0]['var_type'] 
            return data[0][value_col]
            #if value_col == self._var_table_col(adapcast.Pickle) and isinstance(rval, (bytes, bytearray, memoryview)): 
            #    result = adapcast.convert_pickle(rval) # last resort unpickle if not done in select
            #    return result
            #else:
            #    return rval

    def set_var(self, name, value, **kwargs):
        'Save one variable to the database - note a value of None deletes any existing entry'
        if not self._vars_table:
            raise RuntimeError('Variable storage disabled')
        qtable = self.iquote(self._vars_table)
        if value is None:
            params = [name]
            sql = 'DELETE FROM %s WHERE var_name = %s;' % (qtable, self._phchar)
        else:
            obj_type_label = self._obj_type_label(value)
            #if obj_type_col == str(adapcast.Pickle):
            #    value = adapcast.Pickle(value)
            var_type = self._var_type(obj_type_label)
            advalue = self._obj_for_adapting(value)
            params = [name, var_type, advalue]
            #print('insert', params)
            if self.db_type == 'POSTGRESQL' or self.db_type == 'SQLSERVER':
                self._execute_norows('DELETE FROM %s WHERE var_name = %s;' % (qtable, self._phchar), [name]) 
                insertcmd = 'INSERT'
            elif self.db_type == 'MYSQL':
                insertcmd = 'REPLACE'
            else:
                insertcmd = 'INSERT OR REPLACE'
            ph3 = '%s,%s,%s' % (self._phchar, self._phchar, self._phchar) # 3 place holders
            sql = '%s INTO %s (var_name, var_type, %s) VALUES (%s);' % (insertcmd, qtable, self.iquote(var_type, force=True), ph3) 
            # force quote because short type names can overlap with SQL keywords (set)
        self._execute_norows(sql, params)          
        self._commit_if_default(kwargs)
        
    def save_var(self, *args, **kwargs):
        self.set_var(*args, **kwargs)
        
    def all_vars(self, name):
        'Retrieve all saved variables from the database.'
        if not self._vars_table:
            return {}
        data = self.select(table_name = self._vars_table)
        if not data:
            return {}
        else:
            return { d['var_name']: d[d['var_type']] for d in data } 
            
    def clear_vars(self): 
        'Recreate an empty vars table.'
        if not self._vars_table:
            return
        if self.db_type == 'SQLSERVER':
            sql = """IF EXISTS ( SELECT * FROM information_schema.tables WHERE table_name = %s )
                DROP TABLE %s;""" % (self._phchar, self.iquote(self._vars_table))
            self._execute_norows(sql, [self._vars_table]) 
        else:
            sql = 'DROP TABLE IF EXISTS %s;' % self.iquote(self._vars_table)
            self._execute_norows(sql)
        self._check_or_create_vars_table() # includes an implicit commit
        
    def _register_adapter(self, python_type, adapt_callable):
        if self.db_type == 'POSTGRESQL':
            self._dbmodule.extensions.register_adapter(python_type, adapt_callable) # a class
        else:
            self._dbmodule.register_adapter(python_type, adapt_callable) # a function
            
    def _register_adapters(self):
        for python_type, adapt in self._adapt_map.items():
            self._register_adapter(python_type, adapt)
    
    def _register_converter(self, col_type, convert_function): 
        if self.db_type == 'POSTGRESQL':
            self.cursor.execute("SELECT NULL::%s" % col_type) # get oid for the column type
            base_oid = self.cursor.description[0][1]
            #print(base_oid, col_type.upper())
            new_typecaster = self._dbmodule.extensions.new_type((base_oid,), col_type.upper(), convert_function)
            self._dbmodule.extensions.register_type(new_typecaster)
        else:
            self._dbmodule.register_converter(col_type, convert_function)  

    def _register_converters(self, alt=False):
        jcols = [ 'json', 'jsontuple', 'jsonset' ]
        dcols = [ 'date', 'datetime', 'timestamp', 'time' ]
        bcols = [ 'boolean' ]
        for col_type, convert in self._convert_map.items():
            if alt and self._convert_alt.get(col_type):
                if (self._json_str_output and col_type in jcols) or \
                       (self._dates_str_output and col_type in dcols) or \
                       (self._bool_int_output and col_type in bcols):
                    self._register_converter(col_type, self._convert_alt[col_type]) 
                    continue
            self._register_converter(col_type, convert) 
                
    """def _json_string_converters(self, enable=False):
        for col_type, convert in self._convert_map.items():
            if not col_type.startswith('json'):
                continue
            if enable:
                if col_type in self._convert_strings:
                    self._register_converter(col_type, self._convert_strings[col_type])
                else:
                    if self.db_type == 'POSTGRESQL':
                        self._register_converter(col_type, adapcast.convert_pg_text)
                    else:
                        self._register_converter(col_type, adapcast.convert_text) # no processing return JSON as text (unicode) string
            else:
                self._register_converter(col_type, convert) # normal
                
    def _dates_string_converters(self, enable=False):
        for col_type, convert in self._convert_map.items():
            if col_type not in ('date', 'datetime', 'timestamp', 'time'):
                continue
            if enable:
                if col_type in self._convert_strings:
                    self._register_converter(col_type, self._convert_strings[col_type])
                else:
                    if self.db_type == 'POSTGRESQL':
                        self._register_converter(col_type, adapcast.convert_pg_text)
                    else:
                        self._register_converter(col_type, adapcast.convert_text) # no processing date is ISO text (unicode) string
            else:
                self._register_converter(col_type, convert) # normal"""
    
    def insert(self, data, table_name=None, replace=False, create=False, **kwargs): 
        # does create table AND column add only if there is an exception cause by missing table or column
        # this is because create/alter table is not usually necessary and it imposes a commit within the insert/replace methods
        # In sqlite/mysql non-query/non-modification commands such as CREATE/ALTER TABLE/PRAGMA cause an implicit commit
        # NOTE this is not an upsert - existing stored values are deleted if they do not exist in the supplied data 
        table_name = table_name if table_name else self._data_table
        if len(data) == 0 and not hasattr(data, 'keys'):
            return []
            
        insertcmd = 'INSERT'
        if replace:
            if self.db_type == 'SQLITE':
                insertcmd = 'INSERT OR REPLACE'
            elif self.db_type == 'MYSQL':
                insertcmd = 'REPLACE'
            
        if create or table_name not in self._tables: 
            self.create_table(table_name=table_name, data=data) 
            # creates table only if it does not exist, but includes _check_and_add_columns() which internally runs _clean_data()
            # very wasteful to do this routinely, which is why the 'create' flag is off by default and can be
            # triggered by an exception below        
        
        this_data = self._clean_data(data) # Turns it into a list of lists of (key, value) tuples
        # None values ARE removed because the default for any missing field will be NULL = full row replace
        
        table_keys = self.key_columns(table_name) if self.db_type == 'POSTGRESQL' or self.db_type == 'SQLSERVER' else [] 
        
        if self._has_rowids:
            rowids = [] # rowids of inserted rows
        else:
            row_total = 0
        
        try:
            for row in this_data:
                # row is a list of (key, value) tuples 
                fields = [ self.iquote(pair[0]) for pair in row ] 
                values = [ self._obj_for_adapting(pair[1]) for pair in row ] # wrap in Adapter class here if necessary
                pholders = ','.join([self._phchar for f in fields]) # place holders

                if self.db_type == 'POSTGRESQL' or self.db_type == 'SQLSERVER':
                
                    if replace and table_keys: # delete any pre-existing row with the same keys
                        row_keys = [ f[0] for f in row if f[0] in table_keys ]
                        row_key_values = [ f[1] for f in row if f[0] in table_keys ]
                        conditions = [ self.iquote(k) + ' = ' + self._phchar for k in row_keys ] 
                        sqlsubs = (self.iquote(table_name), ' AND '.join(conditions))
                        sql = 'DELETE FROM %s WHERE %s;' % sqlsubs
                        self._execute_norows(sql, row_key_values)
                
                    sqlsubs = (self.iquote(table_name), ', '.join(fields), pholders)
                    if self._has_rowids:
                        if self.db_type == 'POSTGRESQL':
                            sql = 'INSERT INTO %s (%s) VALUES (%s) RETURNING rowid;' % sqlsubs
                            self._execute_norows(sql, values) 
                            rowid = self.cursor.fetchone()[0]
                        elif self.db_type == 'SQLSERVER':
                            sql = 'INSERT INTO %s (%s) VALUES (%s);' % sqlsubs
                            self._execute_norows(sql, values) 
                            sql2 = 'SELECT ident_current(%s);' % self.iquote(table_name)
                            rowid = self.cursor.execute(sql2).fetchone()[0] # NB direct cursor execute
                    else:
                        sql = 'INSERT INTO %s (%s) VALUES (%s);' % sqlsubs
                        count = self._execute_norows(sql, values)
                        row_total = row_total if count is None else row_total + count
                
                else:
                
                    sqlsubs = (insertcmd, self.iquote(table_name), ', '.join(fields), pholders)
                    sql = '%s INTO %s (%s) VALUES (%s);' % sqlsubs
                    count = self._execute_norows(sql, values) 
                    if self._has_rowids:
                        rowid = self.cursor.lastrowid
                        #rowid = self.cursor.execute('SELECT last_insert_rowid();').fetchone()[0] # NB direct cursor execute
                    else:
                        row_total = row_total if count is None else row_total + count
                    
                if self._has_rowids and rowid != 0:
                    #print("data, rowid", fields, values, rowid)
                    rowids.append(rowid)
            
        except self._dbmodule.Error as e:
            #print(type(e).__name__, str(e).split(':')[0])
            etype = type(e).__name__
            msg = str(e).split(':')
            msg = msg[1] if self.db_type == 'SQLSERVER' else msg[0]
            #print(etype, str(e), msg)
            if (self.db_type == 'POSTGRESQL' and etype in [ 'UndefinedTable', 'UndefinedColumn']) or \
               (self.db_type == 'MYSQL' and ("1146" in msg or "1054" in msg)) or \
               (self.db_type == 'SQLITE' and ('no such table' in msg or 'no column named' in msg or 'no such column' in msg)) or \
               (self.db_type == 'SQLSERVER' and ('Invalid object' in msg or 'Invalid column' in msg or 'Column not found' in msg)):
                #self.commit() # note this ends transaction + saves any data to other table not yet committed, before create/alter table which would lose data
                self.rollback()
                return self.insert(data=data, table_name=table_name, replace=replace, create=True, **kwargs) # start again
            raise
                
        if self._has_rowids:
            if rowids:
                self._commit_if_default(kwargs)
            return rowids
        else:
            if row_total > 0:
                self._commit_if_default(kwargs)
            return row_total

    def upsert(self, *args, **kwargs): # included for compatibility only - not a real upsert see above
        return self.insert(replace=True, *args, **kwargs)
        
    def save(self, *args, **kwargs): # actually an insert or replace operation
        return self.insert(replace=True, *args, **kwargs)
        
    def delete(self, conditions, table_name=None, params=[], **kwargs):
        """ delete rows from the table in the datastore where conditions apply
        delete without conditions not allowed - but you can force wholesale delete by supplying an always true condition like '1=1' """
        if not conditions:
            return
        table_name = table_name if table_name else self._data_table
        sql = "DELETE FROM %s WHERE %s" % (self.iquote(table_name), conditions)
        count = self._execute_norows(sql, params)          
        self._commit_if_default(kwargs)
        return count
        
    def _clean_data(self, data, remove_none=True):
        # Turns it into a list of lists of (field, value) tuples (+optional step to remove items with None values).
        try:
            data.keys
        except AttributeError:
            try:
                [e for e in data]
            except TypeError:
                raise TypeError('Data must be a mapping (like a dict), or an iterable of field, mapping pairs ')
        else:
            data = [data] # It is a single dictionary

        cleaned = []
        for row in data:

            newrow = []
            fieldset = set()
            for field, value in row.items():
                if remove_none and value is None:
                    continue
                    
                if field in [None, '']:
                    raise ValueError('Field names must not be blank')
                elif not isinstance(field, str):
                    raise ValueError('Field names ("%s") must be of string type not %s.' % (field, type(field)))
                    
                #if obj_type == str(adapcast.Pickle):
                #    #print ('da', type(value))
                #    newrow.append( (field, adapcast.Pickle(value)) )   
                #else:
                #    newrow.append( (field, value) )
                newrow.append( (field, value) )
                fieldset.add(field.lower())
                
            if len(fieldset) != len(newrow):
                raise ValueError('Cannot use the same column name twice.')

            cleaned.append(newrow)
            
        return cleaned

    #def create_function(self, name, num_params, func):
    #    if self.db_type == 'SQLITE':
    #        self.connection.create_function(name, num_params, func)
            
    def create_index(self, columns, table_name=None, if_not_exists = True, unique = False): # implicit commit
        'Create a unique index on the column(s) passed returning the index name'
        if not isinstance(columns, (list, tuple)):
            columns = [ columns ]
        table_name = table_name if table_name else self._data_table
        index_name = simplify(table_name) + '_' + '_'.join(map(simplify, columns))
        index_cmd = 'CREATE UNIQUE INDEX' if unique else 'CREATE INDEX'
        if self.db_type == 'SQLSERVER' or self.db_type == 'MYSQL': # in these two dbs text column indexes have to be fixed width type
            all_columns = self.column_info(table_name)
            for col, col_type in all_columns.items():
                new_col_type = self._index_col_type(col_type)
                #print(col_type, new_col_type)
                if new_col_type != col_type:
                    sqlsubs = (self.iquote(table_name), self.iquote(col), new_col_type) 
                    sql = 'ALTER TABLE %s MODIFY COLUMN %s %s;' if self.db_type == 'MYSQL' else 'ALTER TABLE %s ALTER COLUMN %s %s;'
                    #print(sql % sqlsubs)
                    self._execute_norows(sql % sqlsubs)
                    self.commit(implicit = True)
        if (self.db_type == 'MYSQL' or self.db_type == 'SQLSERVER') and if_not_exists:
            indices = self.indices(table_name)
            if index_name not in indices:
                sql = '%s %s ON %s (%s);' % (index_cmd, self.iquote(index_name), self.iquote(table_name), ', '.join(map(self.iquote, columns)))
                #print(sql)
                self._execute_norows(sql)
            else:
                return index_name
        else:
            index_cmd = index_cmd + ' IF NOT EXISTS' if if_not_exists else index_cmd
            sql = '%s %s ON %s (%s);' % (index_cmd, self.iquote(index_name), self.iquote(table_name), ', '.join(map(self.iquote, columns)))
            #print(sql)
            self._execute_norows(sql) 
        self.commit(implicit = True)
        return index_name
        
    def index(self, *args, **kwargs):
        return self.create_index(*args, **kwargs)
        
    def drop_index(self, index_name, table_name=None, if_exists = False): # implicit commit
        'Create a unique index on the column(s) passed.'
        table_name = table_name if table_name else self._data_table
        index_cmd = 'DROP INDEX'
        if self.db_type == 'SQLSERVER' or self.db_type == 'MYSQL':
            if not if_exists:
                sql = '%s %s ON %s;' % (index_cmd, self.iquote(index_name), self.iquote(table_name))
                #print(sql)
                self._execute_norows(sql)
            else:
                index_cmd = '%s %s ON %s;' % (index_cmd, self.iquote(index_name), self.iquote(table_name))
                if self.db_type == 'MYSQL':
                    sql = 'IF EXISTS (SELECT * FROM information_schema.statistics WHERE table_name = %s AND index_name = %s) ' % (self._phchar, self._phchar) + index_cmd
                else:
                    sql = 'IF EXISTS (SELECT * FROM sys.indexes i INNER JOIN sys.tables AS t ON i.object_id = t.object_id WHERE t.name = %s and i.name = %s) ' % (self._phchar, self._phchar) + index_cmd
                #print(sql)
                self._execute_norows(sql, [table_name, index_name])
        else:
            index_cmd = index_cmd + ' IF EXISTS' if if_exists else index_cmd
            sql = '%s %s;' % (index_cmd, self.iquote(index_name))
            #print(sql)
            self._execute_norows(sql) 
        self.commit(implicit = True)
        
    def indices(self, table_name=None):
        ' alphabetically ordered list of index names for a particular table'
        table_name = table_name if table_name else self._data_table
        if self.db_type == 'POSTGRESQL':
            sql = """SELECT indexname AS name FROM pg_indexes WHERE schemaname = 'public' AND tablename = %s ORDER BY name;""" % self._phchar
        elif self.db_type == 'MYSQL':
            sql = """SELECT DISTINCT index_name AS name FROM information_schema.statistics WHERE table_name = %s ORDER BY name;""" % self._phchar
        elif self.db_type == 'SQLSERVER':
            sql = """SELECT i.name as name FROM sys.indexes i INNER JOIN sys.tables AS t ON i.object_id = t.object_id WHERE i.name is not null and t.name = %s ORDER BY i.name;""" % self._phchar
        else:
            sql = """SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = %s ORDER BY name;""" % self._phchar
        rows = self._execute_rows(sql, [table_name])
        #print (rows)
        return sorted([ text(row[0]) for row in rows ])
        
    def _index_col_type(self, col_type):
        if self.db_type == 'MYSQL' and col_type == 'text':
            return 'varchar(%d)' % self._text_key_width # MySQL key has to have a fixed width
        elif self.db_type == 'SQLSERVER' and 'char(max)' in col_type:
            return col_type.replace('(max)', '(%d)' % self._text_key_width) # SQLServer key has to have a fixed width
        return col_type
        
    def iquote(self, text, force=False): # quote an identfier
        return iquote(text, force=force, qchar=self._qchar)
        
    def nquote(self, text): # nullable/literal
        return nquote(text)
    
    """  def __convert_blob(self, blob, col_type):
        col_type = col_type.split(' ')[0] # use only column type before first space
        if col_type.lower() == 'blob':
            return blob
        if not self._mem_db_conn: # lazy connection to in memory database used for data conversion using registered converters
            self._mem_db_conn = self.sqlite3.connect(":memory:", detect_types=self.sqlite3.PARSE_COLNAMES)
        sql = 'select ? as "x [%s]"' % col_type
        return self._mem_db_conn.execute(sql, (blob,)).fetchone()[0]

      def _cast_data_to_column_type(self, data):
        column_types = self.__column_types(table_name)
        for key,value in data.items():
          if SQLITE_TYPE_MAP[type(key)] != column_types[key]:
            try:
              data[key] = type(key)(value)
            except ValueError:
              raise TypeError(u"Data could not be converted to match the existing '%s' column type." % type(key))"""
              
    def dump_to(self, file_name, **kwargs):
        ' dump a table to a csv file '
        with open(file_name, 'w') as f:
            result = self.select(**kwargs) 
            if result:
                writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC) # note this format assumes the only unquoted fields are numeric values
                writer.writerow(result[0].keys()) 
                for row in result:
                    writer.writerow(row.values())
                
    def load_from(self, file_name, **kwargs):
        ' load data into a table from a csv file - note must have first row as header list '
        with open(file_name, 'r') as f:
            reader = csv.reader(f, quoting=csv.QUOTE_NONNUMERIC) # note this format assumes the only unquoted fields are numeric values
            headers = next(reader) 
            data = []
            for row in reader:
                new_row = []
                for v in row:
                    if isinstance(v, str) and v == '':
                        new_row.append(None)
                    elif isinstance(v, float) and v.is_integer():
                        new_row.append(int(v))
                    else: 
                        new_row.append(v)
                data.append(OrderedDict(list(zip(headers, new_row))))
            if data:
                self.save(data=data, **kwargs)
                
    def sql_dt(self, this_dt): 
        # returns quoted literal or quoted identifier suitable for insertion into SQL for a date
        # if this_dt is null, returns the current date
        poss_date = ISODate.cast(this_dt)
        if poss_date:
            return self.nquote(poss_date) # its a literal
        poss_datetime = ISODateTime.cast(this_dt)
        if poss_datetime:
            return self.nquote(poss_datetime[:10]) # its a literal - but return date part only
        if this_dt: # its an identfier
            return self.iquote(this_dt)
        if self.db_type == 'MYSQL':
            return 'CURDATE()' 
        elif self.db_type == 'POSTGRESQL':
            return 'CURRENT_DATE'
        elif self.db_type == 'SQLSERVER':
            return 'CAST(GETDATE() AS DATE)'
        else:
            return "date('now')"
            
    def sql_dtm(self, this_dtm): 
        # returns quoted literal or quoted identifier suitable for insertion into SQL for a datetime
        # if this_dtm is null, returns the current datetime
        if getattr(self, '_sqlite_t_sep', False):
            poss_datetime = ISODateTTime.cast(this_dtm) # use 'T' date time separator
        else:
            poss_datetime = ISODateTime.cast(this_dtm) # use space date time separator by default
        if poss_datetime:
            return self.nquote(poss_datetime) # it's a literal
        poss_date = ISODate.cast(this_dtm)
        if poss_date:
            if getattr(self, '_sqlite_t_sep', False):
                return self.nquote(poss_date + 'T' + '00:00:00') # its a literal
            else:
                return self.nquote(poss_date + ' ' + '00:00:00') # its a literal
        if this_dtm: # its an identfier
            return self.iquote(this_dtm)
        if self.db_type == 'MYSQL':
            return 'NOW()' 
        elif self.db_type == 'POSTGRESQL':
            return 'LOCALTIMESTAMP'
        elif self.db_type == 'SQLSERVER':
            return 'GETDATE()'
        else:
            return "datetime('now')"

    def sql_dt_inc(self, this_dt, inc=0): 
        # return string suitable for insertion into SQL for a date literal or identifier to be incremented by a set number of days
        dt = self.sql_dt(this_dt)
        if not inc:
            return dt
        if self.db_type == 'MYSQL':
            sql = 'DATE_ADD(%s, INTERVAL %-d DAY)' % (dt, inc)
        elif self.db_type == 'POSTGRESQL':
            if inc < 0:
                sql = "(%s - (INTERVAL '%d day'))" % (dt, -inc)
            else:
                sql = "(%s + (INTERVAL '%d day'))" % (dt, inc)
        elif self.db_type == 'SQLSERVER':
            sql = 'DATEADD(day, %d, %s)' % (inc, dt)
        else:
            if not this_dt:
                sql = "date('now', '%+d day')" % inc  
            else:
                sql = "date(%s, '%+d day')" % (dt, inc) 
        return sql

    def sql_before_dt(self, ref_dt, cmp_dt): 
        # SQL expression to evaluate true only if the reference date is before the comparison date
        return self.sql_dt(ref_dt) + ' < ' + self.sql_dt(cmp_dt)

    def sql_after_dt(self, ref_dt, cmp_dt): 
        # SQL expression to evaluate true only if the reference date is after the comparison date
        return self.sql_dt(ref_dt) + ' > ' + self.sql_dt(cmp_dt)


