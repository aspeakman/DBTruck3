import unittest
from datetime import datetime, date, time
import time as timetime
import os
from math import floor

from dbtruck import Store, Pickle, ISODate, ISODateTime

import dbtruck

import test_settings as settings

def connect_string(testname):
    use_rowids = settings.USE_ROWIDS
    t_sep = settings.SQLITE_T_SEPARATOR
    connect_s = settings.CONNECT_STRING
    if connect_s.startswith('mysql://') or connect_s.startswith('postgresql://') \
            or connect_s.startswith('postgres://') or ';' in connect_s:
        return connect_s, t_sep, use_rowids
    os.makedirs(connect_s, exist_ok=True)
    dbfile = connect_s+os.sep+testname+'.sqlite'
    if os.path.exists(dbfile): 
        os.remove(dbfile)
    return dbfile, t_sep, use_rowids

class TestClass(object):
    avalue = 10

class DBTruckTests(unittest.TestCase):

    def test_store_data(self):  

        weird_thing = { 'blan': None, 1: 2, False: 'true' }
    
        mydata = {
            'uid': 'xxx',
            'atrue': True,
            'afalse': False,
            'number': 999,
            'floating': 0.346,
            'somebytes': b'xaxy',
            'unicode': u'blingblangy\u00e9\u00f8C', # e acute + degree
            'atuple': ( 1, 2, 3 ),
            'aset': { 1, '2', 3.6 },
            'adate': date.today(),
            'adatetime': datetime.now(),
            'atime': datetime.now().time(),
            'jsonobj': { 'uni': u'123 \u0115\u00f8C', 'data': 999.99, 'default': 'def' }, # e caron + degree
            'anobj': TestClass(),
            'strdt': '1987-12-11',
            'strdtm': '1987-12-11T00:00:01.3',
            'strtm': '14:07:01.345678',
            'pickled': Pickle(weird_thing),
            'longstring': """xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx""",
            }
        
        
        connect, t_sep, use_rowids = connect_string(self._testMethodName)
        teststore = Store(connect, sqlite_t_sep=t_sep, has_rowids = use_rowids)
        dbtable = self._testMethodName + '_1'
        teststore.drop(dbtable, if_exists=True)
        teststore.create(table_name=dbtable, data=mydata )
        applics = [ mydata ]  
        teststore.save( data=applics, table_name=dbtable, commit=True)
        retrieved = teststore.select(table_name=dbtable) 
        r = retrieved[0]
        #print(r)
        self.assertIsInstance(r['uid'], str, 'Not preserving string data')
        self.assertIsInstance(r['atrue'], bool, 'Not preserving boolean data')
        self.assertIsInstance(r['number'], int, 'Not preserving integer data')
        self.assertIsInstance(r['floating'], float, 'Not preserving float data')
        self.assertIsInstance(r['somebytes'], bytes, 'Not preserving bytes data')
        self.assertIsInstance(r['unicode'], str, 'Not preserving unicode data')
        self.assertIsInstance(r['adate'], date, 'Not preserving date data')
        self.assertIsInstance(r['adatetime'], datetime, 'Not preserving datetime data')
        self.assertIsInstance(r['atime'], time, 'Not preserving time data')
        self.assertIsInstance(r['jsonobj'], dict, 'Not preserving json data')
        self.assertIsInstance(r['strdt'], date, 'Not preserving string date data')
        self.assertIsInstance(r['strdtm'], datetime, 'Not preserving string datetime data')
        self.assertIsInstance(r['strtm'], time, 'Not preserving string time data')
        #self.assertIsInstance(r['intts'], int, 'Not preserving integer unix epoch data')
        self.assertIsInstance(r['aset'], set, 'Not preserving set data')
        self.assertIsInstance(r['atuple'], tuple, 'Not preserving tuple data')
        self.assertIsInstance(r['anobj'], TestClass, 'Not preserving object data')
        self.assertIsInstance(r['pickled'], dict, 'Not preserving pickled objects')
        self.assertIsInstance(r['longstring'], str, 'Not preserving long strings type')
        
        self.assertEqual(r['longstring'], mydata['longstring'], 'Not preserving long strings')
        
        #retrieved2 = teststore.select(table_name=dbtable, fields=('adatetime', 'ts [isoticks]'))
        #self.assertIsInstance(retrieved2[0]['ts'], int, 'Not retrieving integer unix epoch from ISO')
        #retrieved3 = teststore.select(table_name=dbtable, fields=('intts', 'ts [ticksiso]')
        #self.assertIsInstance(retrieved3[0]['ts'], str, 'Not retrieving ISO from integer unix epoch')
        #retrieved2 = teststore.select(table_name=dbtable, fields='adatetime::text as ts') # note default has space separater not 'T'
        #self.assertIsInstance(retrieved2[0]['ts'], str, 'Not casting dates to ISO')
        
        #Placeholders and conditions for booleans
        conditions = 'atrue = ?'; params = [ True ]
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['atrue'], bool, 'Boolean true placeholder not correct instance')
        self.assertEqual(retrieved[0]['atrue'], True, 'Boolean true placeholder not working')
        conditions = 'afalse = ?'; params = [ False ]
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['afalse'], bool, 'Boolean false placeholder not correct instance')
        self.assertEqual(retrieved[0]['afalse'], False, 'Boolean false placeholder not working')
        
        #Placeholders and conditions for dates + datetimes
        conditions = 'adate = ?'; params = [ mydata['adate'] ]
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['adate'], date, 'Date placeholder not correct instance')
        self.assertEqual(retrieved[0]['adate'], mydata['adate'], 'Date placeholder not working')
        conditions = 'adate = ?'; params = [ mydata['adate'].isoformat() ]
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['adate'], date, 'ISO Date placeholder not correct instance')
        self.assertEqual(retrieved[0]['adate'], mydata['adate'], 'ISO Date placeholder not working')
        
        conditions = 'adatetime = ?'; params = [ mydata['adatetime'] ]
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['adatetime'], datetime, 'Datetime placeholder not correct instance')
        self.assertEqual(retrieved[0]['adatetime'], mydata['adatetime'], 'Datetime placeholder not working')
        if t_sep:
            conditions = 'adatetime = ?'; params = [ mydata['adatetime'].isoformat('T') ] # note T separator
        else:
            conditions = 'adatetime = ?'; params = [ mydata['adatetime'].isoformat(' ') ] # note space separator
        retrieved = teststore.select(table_name=dbtable, conditions=conditions, params=params) 
        self.assertIsInstance(retrieved[0]['adatetime'], datetime, 'ISO Datetime placeholder not correct instance')
        self.assertEqual(retrieved[0]['adatetime'], mydata['adatetime'], 'ISO Datetime placeholder not working')
        
        #self.assertIsInstance(r['adate'], date, 'Not retrieving date data')
        #self.assertIsInstance(r['adatetime'], datetime, 'Not retrieving datetime data')
        #self.assertIsInstance(r['atime'], time, 'Not retrieving time data')
        
        teststore.close()
        
        teststore = Store(connect, json_str_output = True, dates_str_output = True, bool_int_output = True, sqlite_t_sep=t_sep, has_rowids = use_rowids)
        dbtable = self._testMethodName + '_2'
        teststore.drop(dbtable, if_exists=True)
        teststore.create(table_name=dbtable, data=mydata)
        applics = [ mydata ]  
        teststore.save( data=applics, table_name=dbtable, commit=True)
        retrieved = teststore.select(table_name=dbtable) 
        r = retrieved[0]
        self.assertIsInstance(r['uid'], str, 'Not preserving string data')
        self.assertNotIsInstance(r['atrue'], bool, 'Not converting boolean data to integer') # note boolean is int so isinstance(int) does not work
        self.assertIsInstance(r['number'], int, 'Not preserving integer data') 
        self.assertIsInstance(r['floating'], float, 'Not preserving float data')
        self.assertIsInstance(r['somebytes'], bytes, 'Not preserving bytes data')
        self.assertIsInstance(r['unicode'], str, 'Not preserving unicode data')
        self.assertIsInstance(r['atuple'], str, 'Not converting tuple data to dump unicode string')
        self.assertIsInstance(r['jsonobj'], str, 'Not converting json data to dump unicode string')
        self.assertIsInstance(r['anobj'], TestClass, 'Not preserving object data')
        self.assertIsInstance(r['pickled'], dict, 'Not preserving pickled objects')
        self.assertIsInstance(r['longstring'], str, 'Not preserving long strings')
        self.assertIsInstance(r['adate'], str, 'Not converting date data to string')
        self.assertIsInstance(r['adatetime'], str, 'Not converting datetime data to string')
        self.assertIsInstance(r['atime'], str, 'Not converting time data to string')
        self.assertIn('T', r['adatetime'], 'Not formatting datetime string 1 with T separator') 
        self.assertIsInstance(r['strdt'], str, 'Not preserving string date data')
        self.assertIsInstance(r['strdtm'], str, 'Not preserving string datetime data')
        self.assertIsInstance(r['strtm'], str, 'Not preserving string time data')
        self.assertIn('T', r['strdtm'], 'Not formatting datetime string 2 with T separator') 
        
    def test_date_functions(self):   

        applics = [
        { 'authority': 'Dummy', 'uid': 'applic0', 'start_date': '1974-05-01', 'decided_date': '1974-06-01',
            'date_scraped': '1974-05-23T12:06:06' }, # scraped one week before decided_date
        { 'authority': 'Dummy', 'uid': 'applic1', 'start_date': '1974-05-01', 'decided_date': '1974-06-01',
            'date_scraped': '1974-06-01T12:06:06' }, # scraped on decided date   
        { 'authority': 'Dummy', 'uid': 'applic2', 'start_date': '1974-05-01', 'decided_date': '1974-06-01',
            'date_scraped': '1974-06-04T12:06:06' }, # scraped 4 days after decided_date
        { 'authority': 'Dummy', 'uid': 'applic3', 'start_date': '1974-05-01', 'decided_date': '1974-06-01',
            'date_scraped': '1974-06-18T12:06:06' }, # scraped two weeks after decided_date
        { 'authority': 'Dummy', 'uid': 'applic4', 'start_date': '1971-05-01', 'decided_date': '1971-06-01',
            'date_scraped': '1971-06-18T12:06:06' }, # scraped two weeks after decided_date (but in 1971)
            ]
    
        dbtable = self._testMethodName
        connect, t_sep, use_rowids = connect_string(dbtable)
        teststore = Store(connect, sqlite_t_sep=t_sep, has_rowids = use_rowids)
        teststore.drop(dbtable, if_exists=True)
        teststore.create(table_name=dbtable, data={ 'authority': 'dummy', 'uid': 'xxx', 'date_scraped': datetime.now(), 
                'start_date': date.today(), 'decided_date': date.today(),
                } )
        
        teststore.save( data=applics, table_name=dbtable, commit=True)
        
        fixed_date_tests = { # all match 3 records above
            'ISO date': '1974-06-01', 
            'ISO datetime T separator': '1974-06-01T01:01:01',
            'ISO datetime space separator': '1974-06-01 01:01:01',
            'date object': date(1974, 6, 1),
            'datetime object': datetime(1974,6,1,1,1,1),
        }
        for fail_msg, val in fixed_date_tests.items():
            conditions = "date_scraped > %s" % teststore.sql_dt(val) 
            #print (conditions)
            result = teststore.select(table_name=dbtable, conditions=conditions )
            self.assertEqual(len(result), 3, 'SQL fixed date comparison using %s failing' % fail_msg)
            conditions = "date_scraped > %s" % teststore.sql_dtm(val) 
            #print (conditions)
            result = teststore.select(table_name=dbtable, conditions=conditions )
            self.assertEqual(len(result), 3, 'SQL fixed datetime comparison using %s failing' % fail_msg)
            if teststore.db_type != 'SQLITE' or ('T separator' in fail_msg and t_sep) or \
                    ('space separator' in fail_msg and not t_sep): # datetime separator is significant in SQLite
                conditions = "date_scraped > ?" 
                #print (conditions)
                result = teststore.select(table_name=dbtable, params=[val],conditions=conditions )
                self.assertEqual(len(result), 3, 'SQL placeholder comparison using %s failing' % fail_msg)
        
        sql_week_after_decn = teststore.sql_dt_inc('decided_date', inc=7)
        sql_date_scraped = teststore.sql_dt('date_scraped')
        iso_conditions = "%s < %s" % (sql_date_scraped, sql_week_after_decn) # excludes anything scraped > 1 week after decided date
        result = teststore.select(table_name=dbtable, conditions=iso_conditions)
        self.assertEqual(len(result), 3, 'sql_dt_inc function not working')
        
        if teststore.db_type == 'SQLITE': # does not work in Postgres/mySQL - cannot use 'AS' column aliases in the WHERE clause
            bef_fields = '*, %s AS week_after_decn' % sql_week_after_decn
            bef_conditions = teststore.sql_before_dt('date_scraped', 'week_after_decn') # excludes anything scraped > 1 week after decided date
            result = teststore.select(table_name=dbtable, fields=bef_fields, conditions=bef_conditions)
            self.assertEqual(len(result), 3, 'sql_before_dtm function not working')
        
        teststore.delete( table_name=dbtable, conditions='1=1')
        
        applics = [{ 'authority': 'Dummy', 'uid': 'applic0', 'start_date': '1974-05-01', 'decided_date': '1974-06-01',
            'date_scraped': '1974-05-23T12:06:06' }, # scraped one week before decided_date
            { 'authority': 'Dummy', 'uid': 'applic1', 'start_date': '1974-05-01', 'decided_date': None,
            'date_scraped': '1974-05-23T12:06:06' }, # scraped one week before decided_date
            ]
        teststore.save( data=applics, table_name=dbtable, commit=True)
        
        result = teststore.select(table_name=dbtable, conditions=iso_conditions) # excludes anything scraped > 1 week after decided date
        self.assertEqual(len(result), 1, 'sql_dt_inc function with null date not working')

    def test_store_vars(self):
    
        anobj = TestClass() 
        strvar = u'xxxxx'
        binvar = b'xyx'
        mydata = {
            'uid': 'xxx',
            'atrue': True,
            'number': 999,
            'floating': 0.346,
            'somebytes': b'xaxy', 
            'unicode': u'blingblangy\u00e9\u00f8C', # e acute + degree
            'adate': date.today(),
            'atime': datetime.now().time(),
            'adatetime': datetime.now(),
            'jsonobj': { 'uni': u'123 \u0115\u00f8C', 'data': 999.99, 'default': 'def' }, # e caron + degree
            #'isodate': '1973-01-01', 
            'atuple': ( 1, '2', 3.0 ),
            'aset': { 1, '2', 3.0, 'sss' },
            'strvar': strvar,
            'binvar': binvar,
            'object': anobj,
            }
            
        connect, t_sep, use_rowids = connect_string(self._testMethodName)
        teststore = Store(connect, json_str_output=True, dates_str_output=True, sqlite_t_sep=t_sep, has_rowids = use_rowids) # note the string output settings should be ignored for vars
        teststore.clear_vars()
        
        for k, v in mydata.items():
            teststore.set_var(k, v)
        for k, v in mydata.items():
            storev = teststore.get_var(k)
            #print(v, storev, type(v), type(storev))
            if isinstance(v, TestClass):
                self.assertEqual(type(v), type(storev), 'Failed to set/get identical class types: %s -> %s' % (str(v), str(storev)))
            elif teststore.db_type == 'SQLSERVER' and isinstance(v, time): # ignore microseconds in SQL Server tests = missing
                newv = v.replace(microsecond = 0)
                self.assertEqual(newv, storev, 'Failed to set/get identical variable: %s -> %s' % (str(newv), str(storev)))
            else:
                self.assertEqual(v, storev, 'Failed to set/get identical variable: %s -> %s' % (str(v), str(storev)))
        k = 'newstr'
        teststore.set_var(k, 'testval')
        teststore.set_var(k, 'testval2')
        storev = teststore.get_var(k)
        self.assertEqual('testval2', storev, 'Failed to replace variable')
        teststore.set_var(k, None)
        storev = teststore.get_var(k)
        self.assertIsNone(storev, 'Failed to clear variable')
        
    def test_save_data(self):    
    
        connect, t_sep, use_rowids = connect_string(self._testMethodName)
        teststore = Store(connect, sqlite_t_sep=t_sep, has_rowids = use_rowids)
        dbtable = self._testMethodName + '_1'
        teststore.drop(dbtable, if_exists=True)
        
        firstdata = {  'field1': 'xxx', 'field2': 'yyy' }
        seconddata = {  'field1': 'xxx', 'field3': 'zzz' }
        
        teststore.save( data=firstdata, table_name=dbtable, commit=True)
        teststore.save( data=seconddata, table_name=dbtable, commit=True)
        
        count = teststore.count(table_name=dbtable) 
        icol = teststore.column_info(table_name=dbtable) 
        self.assertEqual(count, 2, 'Failed to create unkeyed table from data')
        if use_rowids:
            self.assertEqual(len(icol), 4, 'Failed to add new columns from data') # includes rowid column
        else:
            self.assertEqual(len(icol), 3, 'Failed to add new columns from data') # no rowid column
        
        dbtable = self._testMethodName + '_2'
        teststore.drop(dbtable, if_exists=True)
        
        sampledata = { 'key1': '1', 'key2': '2' }
        
        teststore.create(table_name=dbtable, data=sampledata, keys = [ 'key1', 'key2' ] )
        count = teststore.count(table_name=dbtable) 
        icol = teststore.column_info(table_name=dbtable) 
        self.assertEqual(count, 0, 'Failed to create keyed table from data')
        if use_rowids:
            self.assertEqual(len(icol), 3, 'Failed to add key columns from data') # includes rowid column
        else:
            self.assertEqual(len(icol), 2, 'Failed to add key columns from data') # no rowid column
            
        keydata1 = {  'key1': '1', 'key2': '2', 'field1': 'xxx', 'field2': 'yyy' }
        keydata2 = {  'key1': '1', 'key2': '2', 'field1': 'aaa', 'field2': 'bbb' }
        keydata3 = {  'key1': '1', 'key2': '3', 'field1': 'xxx', 'field2': 'yyy' }
        
        teststore.save( data=keydata1, table_name=dbtable, commit=False)
        teststore.save( data=keydata2, table_name=dbtable, commit=False)
        teststore.save( data=keydata3, table_name=dbtable, commit=False)
        teststore.commit()
        
        matched = {  'key1': '1', 'key2': '2' }
        retrieved = teststore.match_select(matched, table_name=dbtable)
        r = retrieved[0]
        if use_rowids:
            self.assertEqual(len(r), 5, 'Failed to replace record') # first record replaced + includes rowid
        else:
            self.assertEqual(len(r), 4, 'Failed to replace record') # first record replaced 
        self.assertEqual(r['field2'], 'bbb', 'Failed to update record') # new record overwritten
        
        keydata4 = {  'key1': '1', 'key2': '2', 'field3': 'xxx', 'field4': 'yyy' }
        teststore.save( data=keydata4, table_name=dbtable, commit=True) # exception - keys do not match
        retrieved = teststore.match_select(matched, table_name=dbtable)
        r = retrieved[0]
        if use_rowids:
            self.assertEqual(len(r), 7, 'Failed to add new fields to old record') # first record overwritten again + includes rowid
        else:
            self.assertEqual(len(r), 6, 'Failed to add new fields to old record') # first record overwritten again 
        self.assertIsNone(r['field2'], 'Failed to replace old fields with null') # first record overwritten
        
        dbtable = self._testMethodName + '_3'
        teststore.drop(dbtable, if_exists=True)
        
        weird_col_name = """no^[hs!'`e]?''sf_"&'"""
        unicode_col_name = u'blingblangy\u00e9\u00f8C' # e acute + degree
        firstdata = { 'key1': '1', unicode_col_name: '2', 'key3': '1', weird_col_name: '2' }
        
        teststore.save( data=firstdata, table_name=dbtable)
        
        cols = teststore.columns(table_name=dbtable)
        if use_rowids:
            self.assertEqual(len(cols), 5, 'Failed to create table from data with default commit') # includes rowid
        else:
            self.assertEqual(len(cols), 4, 'Failed to create table from data with default commit') # no rowid
        self.assertIn(weird_col_name, cols, 'Failed to create quoted column with complex name') 
        self.assertIn(unicode_col_name, cols, 'Failed to create quoted column with unicode name') 
        
        dbtable = 'dbtruckdata'
        teststore.drop(dbtable, if_exists=True)
        
        r1 = teststore.save( {"name":"Thomas","surname":"Levine"} )
        r2 = teststore.save( [{"surname": "Smith"}, {"surname": "Jones", "title": "Mr"}] ) 
        if use_rowids:
            self.assertEqual(r1, [ 1 ], 'Failed to return rowid from single insert') 
            if teststore.db_type == 'SQLITE':
                self.assertEqual(r2, [ 2, 3 ], 'Failed to return rowids on multiple insert') 
            else:
                self.assertEqual(r2, [ 3, 4 ], 'Failed to return rowids on multiple insert') # rowid increments after successful insert (surname=Smith) but then gets rolled back
        else:
            self.assertEqual(r1, 1, 'Failed to return count from single insert') 
            self.assertEqual(r2, 2, 'Failed to return count on multiple insert') 
        #odata = teststore.dump()
        #print(dict(odata[1]))
        
        dbtable = 'dbtruckdata'
        teststore.drop(dbtable, if_exists=True)
        
        r3 = teststore.save( [ {"name":"Thomas"}, {"surname":"Levine"}, {"title": "Mr"} ] ) # make 3 columns in successive insertions
        if use_rowids:
            self.assertEqual(r3, [ 1, 2, 3 ], 'Failed to return rowids on successive inserts') # rowid increments after create col/table
        else:
            self.assertEqual(r3, 3, 'Failed to return count from successive inserts') 
        
        dbtable = self._testMethodName + '_4'
        teststore.drop(dbtable, if_exists=True)
        
        data = [ {  'field1': 'xxx', 'field2': 'yyy' },
            { 'field1': 'xxx', 'field3': 'zzz' },
            {'field1': 'zz', 'field3': 'bb' }            ]
        
        inserted = teststore.save( data=data, table_name=dbtable, commit=True)
        if use_rowids:
            self.assertEqual(len(inserted), 3, 'Failed to return rowids of inserted data')
        else:
            self.assertEqual(inserted, 3, 'Failed to return count of inserted data')
        deleted = teststore.delete(table_name=dbtable, conditions='1=1')
        self.assertEqual(deleted, 3, 'Failed to return count of deleted data')
    
    def test_select_data(self):    
    
        connect, t_sep, use_rowids = connect_string(self._testMethodName)
        teststore = Store(connect, sqlite_t_sep=t_sep, has_rowids = use_rowids)
        
        # booleans
        dbtable = self._testMethodName + '_1'
        teststore.drop(dbtable, if_exists=True)
        data = {  'atrue': True, 'afalse': True, 'anull': True  }
        teststore.create_table( data=data, table_name=dbtable)
        data = {  'atrue': True, 'afalse': False, 'anull': None  }
        teststore.insert( data=data, table_name=dbtable, commit=True)

        if teststore.db_type == 'SQLSERVER':
            selected = teststore.select(table_name=dbtable, conditions='atrue=1')
            self.assertEqual(len(selected), 1, 'Failed to select boolean true as 1')
            selected = teststore.select(table_name=dbtable, conditions='atrue=0')
            self.assertEqual(len(selected), 0, 'Failed to ignore boolean true as 0')
            
            selected = teststore.select(table_name=dbtable, conditions='afalse=0')
            self.assertEqual(len(selected), 1, 'Failed to select boolean false as 0')
            selected = teststore.select(table_name=dbtable, conditions='afalse=1')
            self.assertEqual(len(selected), 0, 'Failed to ignore boolean false as 1')
            
            selected = teststore.select(table_name=dbtable, conditions='anull=1')
            self.assertEqual(len(selected), 0, 'Failed to ignore null with =1 boolean test')
            selected = teststore.select(table_name=dbtable, conditions='anull=0')
            self.assertEqual(len(selected), 0, 'Failed to ignore null with =0 boolean tes')
        else:
            selected = teststore.select(table_name=dbtable, conditions='atrue')
            self.assertEqual(len(selected), 1, 'Failed to select boolean true as true')
            selected = teststore.select(table_name=dbtable, conditions='not atrue')
            self.assertEqual(len(selected), 0, 'Failed to ignore boolean true as not true')
            
            selected = teststore.select(table_name=dbtable, conditions='not afalse')
            self.assertEqual(len(selected), 1, 'Failed to select boolean false as not true')
            selected = teststore.select(table_name=dbtable, conditions='afalse')
            self.assertEqual(len(selected), 0, 'Failed to ignore boolean false as true')
            
            selected = teststore.select(table_name=dbtable, conditions='anull')
            self.assertEqual(len(selected), 0, 'Failed to ignore null with implicit boolean true')
            selected = teststore.select(table_name=dbtable, conditions='not anull')
            self.assertEqual(len(selected), 0, 'Failed to ignore null with implicit boolean false')
        
        #list_select and dict_select
        dbtable = self._testMethodName + '_2'
        teststore.drop(dbtable, if_exists=True)
        
        data = [ 
            { 'surname': 'Brunel', 'forename': 'Isambard' },
            { 'surname': 'Brunel', 'forename': 'Mark' },
            { 'surname': 'Stephenson', 'forename': 'Robert' }, 
            { 'surname': 'Watt', 'forename': 'James' },
            ]
        teststore.insert( data=data, table_name=dbtable, commit=True)
        selected = teststore.list_select(key_field='surname', match_list=[ 'Brunel', 'Watt'], table_name=dbtable)
        self.assertEqual(len(selected), 3, 'Failed to select using list')
        selected = teststore.match_select(match_dict={ 'surname': 'Brunel', 'forename': 'Isambard' }, table_name=dbtable)
        self.assertEqual(len(selected), 1, 'Failed to select using dict')
        
        #indexes
        #print(teststore.column_info(dbtable))
        teststore.create_index(['surname', 'forename'], table_name=dbtable, unique=True)
        inds = teststore.indices(table_name=dbtable)
        self.assertEqual(len(inds), 1, 'Failed to create index')
        self.assertEqual(inds[0], 'testselectdata2_surname_forename', 'Failed to create named index')
        teststore.drop_index('testselectdata2_surname_forename', table_name=dbtable)
        inds = teststore.indices(table_name=dbtable)
        self.assertEqual(len(inds), 0, 'Failed to drop index')
        

if __name__ == '__main__':
    
    print('Testing connection:', settings.CONNECT_STRING)
    
    unittest.main()