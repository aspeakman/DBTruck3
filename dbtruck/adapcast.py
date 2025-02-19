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
import pickle
import json
from datetime import date, datetime, time
import time as timetime
import re
from sqlite3 import PrepareProtocol

try:
    import psycopg2
    from psycopg2.extras import Json
    from psycopg2.extensions import ISQLQuote, PYDATE, PYDATETIME, PYTIME, AsIs, Binary, QuotedString
    PSYCOPG2 = True
except ImportError:
    PSYCOPG2 = False
    
try:
    import mysql.connector
    from mysql.connector import FieldFlag
    from mysql.connector.conversion import CONVERT_ERROR
    MYSQCON = True
except ImportError:
    MYSQCON = False

DENC = 'utf-8' # default encoding for text values
ISO_DATE_REGEX = re.compile(r'^\d{4}-\d{2}-\d{2}$') # ISO8601 YYYY-MM-DD
ISO_DATETIME_REGEX = re.compile(r'^(\d{4}-\d{2}-\d{2})[\sT](\d{2}:\d{2}:\d{2}(\.\d{1,6})?)$')
ISO_TIME_REGEX = re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d{1,6})?$')

# Adapter functions = adapt/cast the custom Python type into a type that can be stored natively in a database
def adapt_clear(val): # do nothing adapter - bytes assumed
    return val
            
def adapt_int(val): 
    return int(val) if val is not None else None
    
def adapt_boolean(val):
    if val is None: return None
    return 1 if val else 0
    
def adapt_json(val):
    if val is None: return None
    return json.dumps(val, ensure_ascii=True, separators=(',', ':')) # dump output in most compact form
        
def adapt_jsonset(val):
    if val is None: return None
    d = [ v for v in val ]
    return adapt_json(d)
        
def adapt_date(val): 
    return val.isoformat() if val is not None else None
    
def adapt_time(val): 
    return val.isoformat() if val is not None else None 
            
def adapt_datetime(val):
    return val.isoformat(' ') if val is not None else None # impose DBAPI standard space separator (not Python standard 'T')
    
def adapt_t_datetime(val):
    return val.isoformat('T') if val is not None else None # impose non-standard Python standard 'T' for stored dates - use only by SQLITE special cases
        
def adapt_pickle(val):
    return pickle.dumps(val) if val is not None else None # returns bytes
        
# Converter functions = convert/cast a stored object from the database into a custom Python type
# NOTE input to convert_ functions is bytes from database, output is a Python object
# NOTE input to postcast_ functions is an extracted/converted object
# NOTE psycopg2/PostGres compatible typecasters have signature(val, cur)
def convert_clear(val, cur=None): # do nothing converter #  PostGres compatible typecaster
    return val

def convert_text(val): # convert stored bytes to unicode string 
    # not Postgres compatible = no account of database encoding but could be via cursor.connection.encoding?
    return val.decode(encoding=DENC) if val is not None else None
    
def postcast_text(val): # ensure val is a unicode string 
    return val
      
def convert_bytes(val): 
    return bytes(val) if val is not None else None
    
def convert_bytearray(val): 
    return bytearray(val) if val is not None else None
        
def convert_integer(val): 
    return int(val) if val is not None else None
    
def convert_boolean(val):
    return bool(int(val)) if val is not None else None
    
def convert_boolint(val):
    return int(val) if val is not None else None

def convert_json(val, cur=None): #  PostGres compatible typecaster
    return json.loads(val) if val is not None else None # accepts str, bytes or bytearray + unicode as input

def convert_jsonset(val): 
    return set(json.loads(val)) if val is not None else None
        
def convert_jsontuple(val): 
    return tuple(json.loads(val)) if val is not None else None
    
def postcast_jsonset(val): 
    return set(val) if isinstance(val, list) else val
        
def postcast_jsontuple(val): 
    return tuple(val) if isinstance(val, list) else val
    
def convert_isodate(val): # retrieve internal ISO date (bytes) as date object
    return date(*map(int, val.split(b'-'))) if val is not None else None
    
def convert_dateiso(val): # retrieve date object as ISO unicode date
    return val.isoformat() if val is not None else None 

def convert_timeiso(val): # retrieve time object as ISO unicode time
    return convert_dateiso(val)

def convert_isodtime(val): # retrieve internal ISO datetime (bytes) as datetime object
    if val is None: return None
    sep = val[10:11]
    datepart, timepart = val.split(sep) # deals with ' ' or 'T' as separator
    year, month, day = map(int, datepart.split(b'-'))
    timepart_full = timepart.split(b'.')
    hours, minutes, seconds = map(int, timepart_full[0].split(b':'))
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode())) # zero pad to length 6
    else:
        microseconds = 0
    return datetime(year, month, day, hours, minutes, seconds, microseconds)
    
def convert_isotime(val): # retrieve internal ISO time (bytes) as time object
    if val is None: return None
    timepart_full = val.split(b'.')
    hours, minutes, seconds = map(int, timepart_full[0].split(b':'))
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode())) # zero pad to length 6
    else:
        microseconds = 0
    return time(hours, minutes, seconds, microseconds)

def convert_isoiso(val): # retrieve internal ISO datetime as ISO format with ('T') separator
    if val is None: return None
    return convert_text(val).replace(' ', 'T')
    
def postcast_isoiso(val): # enforce ISO format with ('T') separator  (as native string format)
    if val is None: return None
    return val.replace(' ', 'T')
    
def convert_isoticks(val): # retrieve internal ISO datetime as timestamp integer
    if val is None: return None
    ts = convert_isodtime(val)
    return int(timetime.mktime(ts.timetuple()))
    
def convert_ticksdtime(val): # retrieve timestamp integer as datetime object
    return datetime.fromtimestamp(int(val)) if val is not None else None
    
def convert_ticksiso(val): # retrieve timestamp integer as ISO datetime  (in native string format)
    dtm = datetime.fromtimestamp(int(val))
    return dtm.isoformat('T') if val is not None else None
    
def convert_dtimeiso(val): # retrieve datetime object as unicode ISO datetime  ('T' separator)
    return val.isoformat('T') if val is not None else None 
        
def convert_dtimeticks(val): # retrieve datetime object as timestamp integer - also DBAPI TimestampFromTicks
    return int(timetime.mktime(val.timetuple())) if val is not None else None
    
def convert_pickle(val): 
    return pickle.loads(val)
    
def postcast_pickle(val): 
    return pickle.loads(val) if isinstance(val, (bytes, bytearray, memoryview)) else val
 
class Adapter(object): 
    # base adapter container class conforming to PostGres ISQLQuote wrapper
    # which means (for Postgres) no need to register an adapter if supplying an object in this wrapper for storage in the database
    # and to SQLite PrepareProtocol
    
    _wrap_name = None
   
    def __init__(self, obj):
        cast_obj = self.__class__.cast(obj)
        if cast_obj is not None:
            self.obj = cast_obj
        else:
            raise ValueError('%s cannot be adapted' % str(obj))
            
    @staticmethod
    def adapt(val): # default is to return whatever is supplied
        return val
        
    @staticmethod
    def cast(obj): # if possible cast object to acceptable version for wrapping, otherwise None
        return None
        
    @staticmethod
    def adapt(val): # default is to return whatever is supplied
        return val if val is None else Adapter(val).adapted() 
        
    def adapted(self): # return adapted version of wrapped object
        return self.obj
        
    def __conform__(self, proto): 
        if PSYCOPG2 and proto is ISQLQuote:
            return self
        elif proto is PrepareProtocol:
            return self.adapted()
        
    def getquoted(self): # output is for direct insertion in SQL statement (result should be a quoted bytes object)
        return AsIs(self.adapted()).getquoted() # default is to insert directly in SQL (object valid in SQL after str conversion)
      
class Pickle(Adapter): # container for picklable objects suitable for SQL blob field

    _wrap_name = 'pickle'

    @staticmethod
    def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
        if obj is None:
            return None
        try:
            converted = convert_pickle(adapt_pickle(obj))
        except pickle.PickleError:
            return None
        if type(obj) == type(converted):
            return obj
        return None
        
    @staticmethod
    def adapt(val): 
        return val if val is None else Pickle(val).adapted() 
        
    def adapted (self):
        return adapt_pickle(self.obj) # bytes
                    
    def getquoted(self): 
        return Binary(self.adapted()).getquoted() # binary quoted for insertion in SQL (with escape of non-printables) 

class ISODate(Adapter): # container for SQL acceptable unicode ISO format date strings

    _wrap_name = 'date'
    
    @staticmethod
    def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
        if isinstance(obj, str) and ISO_DATE_REGEX.match(obj):
            return obj
        elif isinstance(obj, date) and not isinstance(obj, datetime): # datetimes are dates but not apprpriate here
            return obj.isoformat() 
        return None
        
    @staticmethod
    def adapt(val):
        return val if val is None else ISODate(val).adapted() 
                
    def getquoted(self): 
        return QuotedString(self.adapted()).getquoted() # value quoted for insertion in SQL
        
class ISODateTime(Adapter): # container for SQL acceptable unicode ISO format datetime strings

    _wrap_name = 'datetime'
    _sep_char = ' '

    @staticmethod
    def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
        if isinstance(obj, str):
            matched = ISO_DATETIME_REGEX.match(obj) # iso format + ascii
            if matched: 
                return matched.group(1) + ' ' + matched.group(2) # impose DBAPI standard space separator (not Python standard 'T')
        elif isinstance(obj, datetime):
            return obj.isoformat(' ') # impose DBAPI standard space separator (not Python standard 'T')
        return None
        
    @staticmethod
    def adapt(val):
        return val if val is None else ISODateTime(val).adapted() 
                
    def getquoted(self): 
        return QuotedString(self.adapted()).getquoted() # value quoted for insertion in SQL
        
class ISODateTTime(ISODateTime): # container for SQL non-standard unicode ISO format datetime strings (stored with 'T' separator)

    @staticmethod
    def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
        if isinstance(obj, str):
            matched = ISO_DATETIME_REGEX.match(obj) # iso format + ascii
            if matched: 
                return matched.group(1) + 'T' + matched.group(2) # impose DBAPI standard space separator (not Python standard 'T')
        elif isinstance(obj, datetime):
            return obj.isoformat('T') # impose DBAPI standard space separator (not Python standard 'T')
        return None
        
class ISOTime(Adapter): # container for SQL acceptable unicode ISO format time strings

    _wrap_name = 'time'

    @staticmethod
    def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
        if isinstance(obj, str) and ISO_TIME_REGEX.match(obj):
            return obj
        elif isinstance(obj, time):
            return obj.isoformat() 
        return None
        
    @staticmethod
    def adapt(val):
        return val if val is None else ISOTime(val).adapted() 
                
    def getquoted(self): 
        return QuotedString(self.adapted()).getquoted() # value quoted for insertion in SQL
            
#class TicksInt(Adapter): # container for SQL acceptable integer seconds since epoch (ticks)
#
#    @classmethod
#    def cast(cls, obj): # if possible cast supplied object to acceptable version
#        if isinstance(obj, (float, int)) and obj > 0:
#            return int(obj)
#        elif isinstance(obj, datetime):
#            return int(timetime.mktime(obj.timetuple()))
#        elif isinstance(obj, str) and ISODateTime.ISO_DATETIME_REGEX.match(obj):
#            dtm_obj = convert_datetime(obj)
#            return int(timetime.mktime(dtm_obj.timetuple()))
#        return None

SQLITE_CREATE_SEQ = ( 
    # an ordered sequence of 3-tuples which determines the database column types created for each Python sample object supplied
    # 1st element is the type - either a Python base type OR the type of an Adapter class with a cast() static method
    # 2nd element is the underlying column affinity (storage type)
    # 3rd element (if any) is the derived column type different from the affinity (note column types are declared as 'column_type affinity')
  
    (bool, 'integer', 'boolean'), # before int
    (int, 'integer', None), 
    
    (float, 'real', None),
    
    (ISODate, 'text', 'date'), # matches ISO 8601 strings and date objects
    (ISODateTime, 'text', 'datetime'), # matches ISO 8601 strings and datetime objects
    (ISOTime, 'text', 'time'), # matches ISO 8601 strings and time objects
    
    #(TicksInt: ('integer'), 
    
    # string tests come after ISO string dates tests
    (bytearray, 'blob', 'bytearray'),  
    (bytes, 'blob', 'bytes'),
    (str, 'text', None), 

    (dict, 'text', 'json' ),
    (list, 'text', 'json' ),
    (tuple, 'text', 'jsontuple' ),
    (set, 'text', 'jsonset' ),
    
    # default - matches any picklable object
    (Pickle, 'blob', 'pickle'),
)

SQLITE_ADAPTER_MAP = { 
    # keys are Python object types
    # value is any custom adapt function (Python object to SQL)
    bool: adapt_boolean,
    date: adapt_date,
    datetime: adapt_datetime,
    time: adapt_time,
    dict: adapt_json,
    list: adapt_json,
    tuple: adapt_json,
    set: adapt_jsonset,
}

SQLITE_T_CREATE_SEQ = ( 
    # an ordered sequence of 3-tuples which determines the database column types created for each Python sample object supplied
    # 1st element is the type - either a Python base type OR the type of an Adapter class with a cast() static method
    # 2nd element is the underlying column affinity (storage type)
    # 3rd element (if any) is the derived column type different from the affinity (note column types are declared as 'column_type affinity')
  
    (bool, 'integer', 'boolean'), # before int
    (int, 'integer', None), 
    
    (float, 'real', None),
    
    (ISODate, 'text', 'date'), # matches ISO 8601 strings and date objects
    (ISODateTTime, 'text', 'datetime'), # matches ISO 8601 strings and datetime objects NB imposes non-standard 'T' separator on stored datetimes
    (ISOTime, 'text', 'time'), # matches ISO 8601 strings and time objects
    
    #(TicksInt: ('integer'), 
    
    # string tests come after ISO string dates tests
    (bytearray, 'blob', 'bytearray'),  
    (bytes, 'blob', 'bytes'),
    (str, 'text', None), 

    (dict, 'text', 'json' ),
    (list, 'text', 'json' ),
    (tuple, 'text', 'jsontuple' ),
    (set, 'text', 'jsonset' ),
    
    # default - matches any picklable object
    (Pickle, 'blob', 'pickle'),
)

SQLITE_T_ADAPTER_MAP = { 
    # keys are Python object types
    # value is any custom adapt function (Python object to SQL)
    bool: adapt_boolean,
    date: adapt_date,
    datetime: adapt_t_datetime, # imposes non-standard 'T' separator on stored datetimes
    time: adapt_time,
    dict: adapt_json,
    list: adapt_json,
    tuple: adapt_json,
    set: adapt_jsonset,
}

SQLITE_CONVERTER_MAP = { 
    # keys are derived column types (3rd element in the create entries above, or 2nd if empty)
    # values are custom converter functions (SQL to Python object)
    'bytearray': convert_bytearray,
    'bytes': convert_bytes,
    'boolean': convert_boolean,
    'integer': convert_integer,
    
    'date': convert_isodate,
    'datetime': convert_isodtime,
    'time': convert_isotime,
    
    'json': convert_json,
    'jsontuple': convert_jsontuple,
    'jsonset': convert_jsonset,
    'pickle': convert_pickle,
    
    # not used/ tested - theoretically available as [cast] in select statements
    'isoticks': convert_isoticks,
    'ticksdtime': convert_ticksdtime,
    'ticksiso': convert_ticksiso,

}
                
SQLITE_CONVERT_ALT = { # alternative converters (SQL to Python string/int)
    
    'json': convert_text, # unicode text
    'jsontuple': convert_text,
    'jsonset': convert_text,
    'datetime': convert_isoiso, # enforces 'T' separator on output
    'date': convert_text,
    'time': convert_text,
    'boolean': convert_boolint, # integer

}

if MYSQCON:

    MYSQL_CREATE_SEQ = ( 
        # an ordered sequence of 3-tuples which determines the database column types created for each Python sample object supplied
        # 1st element is the type - either a Python base type OR the type of an Adapter class with a cast() static method
        # 2nd element is the base/column/storage type
        # 3rd element is any cast type - second stage conversion after extraction (stored as a comment)      
        (bool, 'tinyint', None), # before int
        (int, 'int', None), 
        
        (float, 'float', None),
        
        (ISODate, 'date', None), # matches ISO 8601 strings and date objects
        (ISODateTime, 'datetime', None), # matches ISO 8601 strings and datetime objects
        (ISOTime, 'time', None), # matches ISO 8601 strings and time objects
        
        # string tests come after ISO string dates tests
        (bytearray, 'blob', 'bytearray'),  
        (bytes, 'blob', None), 
        (str, 'text', None), 

        (dict, 'json', None),
        (list, 'json', None),
        (tuple, 'json', 'jsontuple'),
        (set, 'json', 'jsonset'),
        
        # default - matches any picklable object
        (Pickle, 'mediumblob', 'pickle'),
        
    )
    
    class DBTruckMySQLConverter(mysql.connector.conversion.MySQLConverter):
    
        _json_str_output = False
        _dates_str_output = False
        _bool_int_output = False
    
        # inherits builtin adapter functions from MySQLConverter as follows 
        # int, long, float, str, unicode, bytes, byearray, bool, date, datetime, time, timedelta, decimal
        # (_type_to_mysql) using __class__.__name__.lower() as type value
        # note datetime enforces space separator
        
        def _unicode_to_mysql(self, value):
            return super()._unicode_to_mysql(value)
            
        def _long_to_mysql(self, value):
            return super()._long_to_mysql(value)
    
        def _dict_to_mysql(self, value):
            return adapt_json(value).encode('ascii')
            
        def _list_to_mysql(self, value):
            return adapt_json(value).encode('ascii')
            
        def _tuple_to_mysql(self, value):
            return adapt_json(value).encode('ascii')
            
        def _set_to_mysql(self, value):
            return adapt_jsonset(value).encode('ascii')
            
        def _pickle_to_mysql(self, value): # value is a wrapper instance (bytes)
            return value.adapted()
            
        def _isodate_to_mysql(self, value): # value is a wrapper instance (text)
            return value.adapted().encode('ascii')
            
        def _isodatetime_to_mysql(self, value): # value is a wrapper instance (text)
            return value.adapted().encode('ascii') # built in date time always with space separator
            
        def _isotime_to_mysql(self, value): # value is a wrapper instance (text)
            return value.adapted().encode('ascii')
            
        #def _ticksint_to_mysql(self, value):
        #    return TicksInt.adapt(value)
            
        # inherits converter functions for these resultset column types (among others) from MySQLConverter
        # STRING (VAR_STRING,JSON), INT (TINY,LONG,INT24), DATETIME (TIMESTAMP), DATE (NEWDATE), FLOAT (DOUBLE), BLOB
        # but difficult because types have no exact match to declared column types used in 'create/alter table'
            
        def _tiny_to_python(self, value, desc=None): # maps to tinyint create table type
            return convert_boolint(value) if self._bool_int_output else convert_boolean(value)
        
        def _long_to_python(self, value, desc=None): # maps to integer create table type
            return convert_integer(value)
            
        def _date_to_python(self, value, dsc=None):
            dt = super()._date_to_python(value, dsc)
            return convert_dateiso(dt) if self._dates_str_output else dt
        
        def _datetime_to_python(self, value, dsc=None):
            dtm = super()._datetime_to_python(value, dsc)
            return convert_dtimeiso(dtm) if self._dates_str_output else dtm # imposes 'T' separator
            
        def _time_to_python(self, value, dsc=None): # original returns timedelta, this returns a time
            if isinstance(value, time):
                return value
            time_val = None
            try:
                if len(value) > 8:
                    (hms, mcs) = value.split(b'.')
                    mcs = int(mcs.ljust(6, b'0'))
                else:
                    hms = value
                    mcs = 0
                tval = [int(i) for i in hms.split(b':')] + [mcs, ]
                if len(tval) < 3:
                    raise ValueError("invalid time format: {} len: {}"
                                     "".format(tval, len(tval)))
                else:
                    try:
                        time_val = time(*tval)
                    except ValueError:
                        return None
            except (IndexError, TypeError):
                raise ValueError(CONVERT_ERROR.format(value=value, pytype="datetime.time"))
            return convert_timeiso(time_val) if self._dates_str_output else time_val
        
        def _json_to_python(self, value, dsc=None):
            jsn = super()._string_to_python(value, dsc)
            return jsn if self._json_str_output else convert_json(jsn)
    
        #def _MEDIUM_BLOB_to_python(self, value, dsc=None):  # does not detect this type 
        #    by = super(DBTruckMySQLConverter, self)._BLOB_to_python(value, dsc)
        #    return convert_pickle(by)
            
    MYSQL_CAST_MAP = { # custom  converters (SQL to Python object) - based on second cast column above, NOTE transforms take place AFTER data is extracted from database
        
        # Second stage - post extraction casting based on stored column comments 
        'pickle': postcast_pickle, 
        'jsontuple': postcast_jsontuple, # see json_str_output - function should not convert if val is not a list
        'jsonset': postcast_jsonset, # see json_str_output - function should not convert if val is not a list
        'bytearray': convert_bytearray, # extracted as bytes
        
    }

if PSYCOPG2:
    #  PostGres compatible converters (typecasters)

    def convert_pg_binary_bytes(val, cur):
        m = psycopg2.BINARY(val, cur) # BINARY typecaster returns memoryview
        return m if m is None else bytes(m)
        
    def convert_pg_binary_pickle(val, cur):
        m = psycopg2.BINARY(val, cur) # BINARY typecaster returns memoryview
        return convert_pickle(m)
                
    def convert_pg_text(val, cur): # also takes account of connection encoding
        return psycopg2.extensions.UNICODE(val, cur) # STRING typecaster is an alias for extensions.UNICODE on Python 3
    
    #def convert_pg_py2str(val, cur): # also takes account of connection encoding
    #    return psycopg2.STRING(val, cur) # STRING typecaster is an alias for UNICODE on Python 3
    
    def convert_pg_integer(val, cur): 
        i = psycopg2.extensions.INTEGER(val, cur) # INTEGER typecaster
        return convert_integer(i)
        
    def convert_pg_bool_int(val, cur): 
        i = psycopg2.extensions.INTEGER(val, cur) # INTEGER typecaster
        return convert_boolint(i)
            
    def convert_pg_date_iso(val, cur): #
        dt = PYDATE(val, cur) # psycopg2.extensions.DATE typecaster is an alias for PYDATE
        return convert_dateiso(dt)
        
    def convert_pg_dtime_iso(val, cur):
        dtm = PYDATETIME(val, cur) # psycopg2.DATETIME typecaster is an alias for PYDATETIME
        return convert_dtimeiso(dtm) # imposes 'T' separator
        
    def convert_pg_time_iso(val, cur):
        tm = PYTIME(val, cur) # psycopg2.extensions,TIME typecaster is an alias for PYTIME
        return convert_timeiso(tm)
        
    #  PostGres compatible adapters (ISQLQuote wrappers)

    class PGJson(Json): # Json customised to use our adapt_json function
    
        _wrap_name = 'json'
    
        @staticmethod
        def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
            if isinstance(obj, (dict, list)):
                return obj
            return None
        
        def dumps(self, obj):
            return adapt_json(obj)
            
    class PGJsonTuple(Json): # Json customised to use our adapt_json function
    
        _wrap_name = 'jsontuple'
    
        @staticmethod
        def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
            if isinstance(obj, (tuple)):
                return obj
            return None
        
        def dumps(self, obj):
            return adapt_json(obj)
            
    class PGJsonSet(Json): # Json customised to use our adapt_jsonset function
    
        _wrap_name = 'jsonset'
        
        @staticmethod
        def cast(obj): # if possible, cast object to acceptable version for wrapping, otherwise None 
            if isinstance(obj, set):
                return obj
            return None
        
        def dumps(self, obj):
            return adapt_jsonset(obj)
            
    POSTGRES_CREATE_SEQ = ( 
        # an ordered sequence of 3-tuples which determines the database column types created for each Python sample object supplied
        # 1st element is the type - either a Python base type OR the type of an Adapter class with a cast() static method
        # 2nd element is the base/column/storage type
        # 3rd element is any cast type - second stage conversion after extraction (stored as a comment)      
        (bool, 'boolean', None), # before int
        (int, 'integer', None), 
        
        (float, 'real', None),
        
        (ISODate, 'date', None), # matches ISO 8601 strings and date objects
        (ISODateTime, 'timestamp', None), # matches ISO 8601 strings and datetime objects
        (ISOTime, 'time', None), # matches ISO 8601 strings and time objects
        
        # string tests come after ISO string dates tests
        (bytearray, 'bytea', 'bytearray'),  
        (bytes, 'bytea', None), 
        (str, 'text', None), 
 
        (PGJson, 'json', None ),
        (PGJsonTuple, 'json', 'jsontuple' ),
        (PGJsonSet, 'json', 'jsonset' ),
        
        # default - matches any picklable object
        (Pickle, 'bytea', 'pickle'),
        
    )
    
    POSTGRES_ADAPTER_MAP = { 
        # keys are Python object types
        # value is afunction return an ISQLQuote adapter (Python object to SQL)
        # none registered because registering adapters is not necessary for objects that have a __conform__ method
    }

    POSTGRES_CONVERTER_MAP = { # default converters (SQL to Python object) - based on first base/storage column above

        # Postgress installed typecasters - function signature has to be adapt(val, cur)
        'bytea': convert_pg_binary_bytes,
        'text': convert_pg_text,        
        'json': convert_json,
        'integer': convert_pg_integer, 
        'date': PYDATE, # or psycopg2.extensions.DATE
        'timestamp': PYDATETIME,  # psycopg2.DATETIME
        'time': PYTIME,  # psycopg2.DATETIME

    }
    
    POSTGRES_CONVERT_ALT = { # alternative converters (SQL to Python string/int)
    
        'json': convert_pg_text, # unicode text
        'timestamp': convert_pg_dtime_iso,
        'date': convert_pg_date_iso,
        'time': convert_pg_time_iso,
        'boolean': convert_pg_bool_int,
    
    }

    POSTGRES_CAST_MAP = { # custom  converters (SQL to Python object) - based on second cast column above, NOTE transforms take place AFTER data is extracted from database
        
        # Second stage - post extraction casting based on stored column comments 
        'pickle': postcast_pickle, # extracted as binary
        'jsontuple': postcast_jsontuple, # see json_str_output - function should not convert if val is not a list
        'jsonset': postcast_jsonset, # see json_str_output - function should not convert if val is not a list
        'bytearray': convert_bytearray,
        
        # these are dates stored as ISO text but this checks if they need to be native ISO strings
        #'isodtiso': postcast_nativestr,
        #'isodtmiso': postcast_isoiso, # enforces 'T' separator on output
        
        #'isoticks': convert_isoticks, # not tested
        #'ticksdtime': convert_ticksdtime,
        #'ticksiso': convert_ticksiso,
        
    }


