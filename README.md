DBTruck3
=========

**DBTruck3** provides a Python base class called Store which is a relaxed schema-less interface 
to data tables and stored metadata in a database.

Currently the default database is SQLite, with the optional use of PostgreSQL (psycopg2), MySQL (mysql.connector)
or SQL Server (mssql_python) instead

DBTruck3 was inspired by and is loosely based on the 2011 ScraperWiki [Dumptruck](https://github.com/scraperwiki/dumptruck) module

Features include:

* Simple tabular input and output
* On-the-fly addition of new columns to a table
* Transparent I/O of most common base types and any other objects that can be pickled
* Independent storage and retrieval of named variables (metadata)
* Configurable I/O conversion of Python data types to/from underlying databases

Quick start
----------

How to install, save data and retrieve using default settings.

### Install

    python -m pip install -U "dbtruck @ git+https://github.com/aspeakman/DBTruck3.git"
    
### Initialize
Open the database connection by initializing a Store object which will create an
underlying SQLite database called "dbtruck.db"

    from dbtruck import Store
    st = Store("dbtruck.db")

### Save
The simplest `insert` call looks like this.

    st.insert({"name":"Thomas","surname":"Levine"})

This saves a new row with "Thomas" in the "name" column and
"Levine" in the "surname" column. It uses the default table "dbtruckdata"
inside the SQLite database "dbtruck.db" (created above). It creates or alters the table
as needed.

You can insert further rows with additional fields by suppling multiple dictionaries.
By default `Store.insert` returns the number of new rows inserted.

    st.insert([{"surname": "Smith"}, {"surname": "Jones", "title": "Mr"}]) 
    ==> 2

If there are UNIQUE constraints on the table (see below) then
`insert` will fail if these constraints are violated. You can use `save` (with
the same syntax) to replace the existing row instead.

### Retrieve
Once the database table contains some data, you can retrieve all data as follows:

    st.dump()

The data come out as a list of ordered dictionaries, with one dictionary per row. Columns 
(dictionary keys) have been created as required, and unknown values are set to `None`. 

    [
    OrderedDict({ 'name': 'Thomas', 'surname': 'Levine', 'title': None }),
    OrderedDict({ 'name': None, 'surname': 'Smith', 'title': None }),
    OrderedDict({ 'name': None, 'surname': 'Jones', 'title': 'Mr' })
    ]

Slow start
-------

### Initialize
You can change the default settings when you initialize the Store object.
For example, if you want the SQLite database file to be called `new-names.db`,
you can use the 'connect_details' argument.

    st = Store(connect_details="new-names.db")

Store has default keyword arguments as follows:

    Store(connect_details, data_table = 'dbtruckdata', vars_table = 'dbtruckvars', default_commit = True,  
            timeout = 5, json_str_output = False, dates_str_output = False, bool_int_output = False, 
            has_rowids = False, text_key_width = 100)

* `connect_details` is the SQlite database file to create or save to; alternatively a URI/connection string 
     can be supplied for PostgreSQL (postgresql://user:password@host:port/database) or MySQL (mysql://user:password@host:port/database)
     or SQL Server (Server=ServerAddress;Database=myDataBase;UID=myUsername;PWD=myPassword;)
* `data_table` is the name of the default data table to use for insert and save methods; default is `dbtruckdata`. 
* `vars_table` is the name of the table to use for `get_var`
    and `save_var` methods (see below); default is `dbtruckvars`. Set it to `None`
    to disable the get_var and save_var methods.
* `default_commit` is whether changes to the database should be committed automatically; default is `True`.
    If it is set to `False`, changes must be committed with the `commit` method or with the `commit` keyword argument.
    If it is set to `True`, you can defer the commit for a particular operation by supplying a `commit=False` keyword argument.
* `timeout` is the timeout parameter for the connection to the underlying database; default is 5 (seconds).
* `json_str_output` - by default Python `dict`, `list`, `tuple` and `set` objects are stored as JSON format strings but returned in the original object 
    form; however if this is set to `True` they are returned as JSON strings
* `dates_str_output` - by default Python date/time objects (`date`, `time`, `datetime`) are returned in the original object 
    form; however if this is set to `True` they are returned as ISO 8601 strings (note Python format with 'T' separator for date times)
* `bool_int_output` - by default boolean values are returned as `True` and `False` values; however if this is set 
    to `True` they are returned as the integer values `1` and `0`
* `has_rowids` - if this is set to `True` a `rowid` column is created in the table and each row is allocated a unique numeric value when stored; default is `False`.
   This is useful if you want to know the order in which data were inserted
* `text_key_width` - sets the fixed size (default 100) of text fields used as keys or in indexes in MySQL and SQL Server databases 
    
Note if you want to use PostgreSQL, MySQL or SQL Server as your underlying database (see `connect_details` above) run one of these commands first:

    python -m pip install 'psycopg2>=2.9.10'
    python -m pip install 'mysql-connector-python>=9.2.0'
    python -m pip install 'mssql-python>=0.13.0'

### Non-default tables
It is not necessary to specify a table name if you only use one table. If not specified
the default table name of `dbtruckdata` will be used. However if you use several tables, you can indicate 
which by supplying a `table_name` to any operation.

    st.insert( {"name":"Thomas","surname":"Levine"}, table_name='people')
    st.dump(table_name='people')

### Inserting multiple rows
You can pass a list of dictionaries to insert multiple rows at once

    data=[
        {"firstname": "Thomas", "lastname": "Levine"},
        {"firstname": "Julian", "lastname": "Assange"}
    ]
    st.insert(data)
    
The normal return from `Store.insert` is the number of rows inserted, but if `has_rowids` is set to
 `True` then the return value is a list of the rowids inserted.

    eg: [ 1, 2 ]

### Complex objects
You can even pass nested structures; dictionaries, tuples,
sets and lists will automatically be converted to JSON format strings and when
queried they will be returned as copies of the original objects 

    data=[
        {"title":"The Elements of Typographic Style","authors":["Robert Bringhurst"]},
        {"title":"How to Read a Book","authors":["Mortimer Adler","Charles Van Doren"]}
    ]
    st.insert(data)
    st.dump()

Other complex objects that can't be JSONified can also be stored.
A class object will be automatically stored in pickled form and other
complex objects can be stored using the `Pickle` adapter. In both cases
the stored object is unpickled automatically and a copy of the original is returned.

    # This fails
    data = {"weirdthing": {range(100): None}}
    st.insert(data)

    # This works
    from dbtruck import Pickle
    data = Pickle({"weirdthing": {range(100): None}))
    st.insert(data)

### Names
Column names and table names automatically get quoted if you pass them without quotes,
so you can use bizarre table and column names, like `no^[hs!'e]?'sf_"&'`

### Retrieving
The `Store.dump` command retrieves an entire table, but you can also use the `select` command 
if you want to retrieve parts of a data table based on some filtering conditions. You can also retrieve 
a subset of columns by specifiying a `fields` list. Example:

    data = st.select(fields = [ 'name', 'surname' ], table_name = 'engineers', conditions = 'surname = ?', params = [ 'Brunel' ] )

Note the `conditions` parameter is passed directly to the WHERE clause of the underlying SQL so it is good practice to substitute any variables 
using `?` place holders, as shown in the example, where the appropriate values are quoted and inserted from the `params` list. (Note the '?' 
place holder required by Store is converted to the appropriate place holder character ('%s') if you are using the `psycopg2` or `mysql-connector-python`
modules).

Alternatively you can leave out `params` and use `Store.iquote` (quotes an identifier) or `Store.nquote` (quotes a literal nullable value)
to do appropriate variable substitution into `conditions` as a Python string.

    surname_field = 'surname'
    chosen_engineer = 'Brunel'
    subst = { 'field': st.iquote(surname_field), 'value': st.nquote(chosen_engineer) }
    conditions = '%(field)s = %(value)s ORDER BY %(field)s' % subst
    data = st.select(fields = [surname_field], table_name = 'engineers', conditions = conditions)

Note you can also add further SQL restrictions to `conditions` eg LIMIT or ORDER BY as in the above example

The data returned from `Store.select` (and `Store.dump`) are a list of ordered dictionaries, one dictionary
per row. Data output values are coerced to appropriate Python types depending
on the settings. If `has_rowids` is set to `True` then each row also has a unique 'rowid' value (starting from 1).

    odata = st.dump()
    [
    OrderedDict({ 'name': 'Thomas', 'surname': 'Levine', 'title': None, 'rowid': 1 }),
    OrderedDict({ 'name': None, 'surname': 'Smith', 'title': None, 'rowid': 2 }),
    OrderedDict({ 'name': None, 'surname': 'Jones', 'title': 'Mr', 'rowid': 3 })
    ]

Varations on `select` are `match_select` (select matches from a list of values) and `list_select` (select matches form a dict of keys and values)

    st.list_select(key_field = 'surname', match_list = [ 'Brunel' ], table_name = 'engineers')
    st.match_select(match_dict = { 'surname': 'Brunel' }, table_name = 'engineers')

### Deleting
The delete operation (`Store.delete`) also requires a `conditions` parameter to specify which rows will be affected. 

    st.delete(table_name = 'engineers', conditions = 'surname = ?', params = [ 'Brunel' ] )

Deleting without conditions is not allowed - but you can force a wholesale delete by supplying an always true condition like '1=1'

Other methods (described below) that can filter records based on 'conditions' and 'params' with ? placeholders are `Store.dump_to`, 
`Store.get_max`, `Store.count`, `Store.get_min`.

### Executing SQL
You can also use normal SQL, for example to retrieve data from the database, or to update fields
in existing rows. However you need to be careful
that the syntax is acceptable to the underlying database and that values are appropriately quoted.

    data = st.execute("SELECT name, surname FROM `engineers` WHERE surname = 'Brunel'")
    date = st.execute('SELECT * from `coal`;')

### Metadata values
You can save and retrieve miscellaneous metadata values using the Store class instance. The `Store.get_var` and 
`Store.save_var` methods are used for this kind of operation.

For example, you can record which page the last run of a script managed to get up to.

    st.save_var('last_page', 27)
    st.get_var('last_page')
    ==> 27

Each variable is stored in a special `vars_table` that you can specify when initializing the Store class.
If you don't specify one, the table is named `dbtruckvars`.

Note: all standard Python objects including int, float, str, dict, bool, bytes, list, date, time and datetime can be stored. 
(see 'I/O details of Python data types' below). Complex objects will also be stored in pickled form.

The `Store.all_vars` method returns all metadata variables and their values as a dict, the `Store.clear_vars` methods deletes them all.

### Creating tables and columns
In the Store class a table can be created on first insert based on the data supplied. Also additional columns 
of the appropriate type are created on the fly when new data is inserted. 

However you can also use `Store.create_table` to create the initial schema, based on a data template. 
For example, if the table `tools` does not exist, the following call will create the table
`tools` with the columns `tool_type` and `weight`, with text and integer types,
respectively. Note that it does not insert the template dictionary values ("jackhammer" and 58)
into the table.

    st.create_table( {"tool_type":"jackhammer", "weight": 58}, table_name="tools" )

If you are concerned about the order of the columns, pass an OrderedDict.

    st.create_table( OrderedDict([("tool_type", "jackhammer"), ("weight", 58)]), table_name="tools" )

The columns will be created in the specified order.

You can define the primary `keys` for a table when you create it (or you can create an equivalent 'unique' index, see below)

    st.create_table( {"tool_type":"jackhammer", "weight": 58}, table_name="tools", keys=['tool_type'] )

### Deleting tables
`Store.drop_table` drops a table. Note you have to specify a `table_name`, so you cannot delete the default table unless you name it.

    st.drop_table(table_name="diesel-engineers")
    
It is an error to try to drop a table if it does not exist. Add the `if_exists` parameter to avoid this.

    st.drop_table(table_name="diesel-engineers", if_exists=True)

### Listing tables and columns
List table names with the `Store.tables` command, columns with `Store.columns` and key columns with `Store.key_columns`

    st.tables()
    st.columns(table_name="diesel-engineers")
    st.key_columns(table_name="diesel-engineers")
    
Use `Store.column_info` to get a dictionary of columns and their data types

    st.column_info(table_name="diesel-engineers")
    
### Saving (insert or replace)
The insert operation fails if you are trying to insert a row with a duplicate key, as in the following example.

    st.create_table( {"tool_type":"jackhammer", "weight": 58}, table_name="tools", keys=['tool_type'] )
    st.insert( {"tool_type":"woodsaw", "weight": 5, "colour": 'blue' } )
    st.insert( {"tool_type":"woodsaw", "weight": 7} ) # this causes an error because a 'woodsaw' entry already exists

An alternative is `Store.save` which completely replaces an existing keyed row with the latest version

    st.save( {"tool_type":"woodsaw", "weight": 5, "colour": 'blue'} )
    st.save( {"tool_type":"woodsaw", "weight": 7} ) # this works, but NOTE it replaces the previous row, so the 'colour' value for 'woodsaw' is now NULL


### Indexes
To create an index, first create a table with the fields you want to index. (See "Creating tables and columns" above.)

Then, use the `Store.create_index` method. For example his will create a non-unique index on the column `tool_type`, returning the index name
based on the supplied table name and columns.

    st.create_index('tool_type', table_name='tools')
    ==> tools_tooltype

Alternatively to create a unique index (equivalent to creating the table with a primary key) use the keyword argument `unique = True`.

    st.create_index( 'tool_type', table_name='tools', unique=True )
    ==> tools_tooltype

You can also specify multi-column indices.

    st.create_index(['tool_type', 'weight'], table_name='tools')
    ==> tools_tooltype_weight

To get a list of created indexes use the `Store.indices` method.
which returns a list of the indices for a particular table.

    st.indices(table_name='tools')
    ==> [ 'tools_tooltype_weight', 'tools_tooltype' ]

To delete an index.

    st.drop_index('tools_tooltype_weight', table_name='tools')

It is an error to try to drop an index if it does not exist. Add the `if_exists` parameter to avoid this.

    st.drop_index('tools_tooltype_weight', table_name='tools', if_exists=True)

### Delayed commits (atomic operations / transactions)
By default, the `insert`, `save`, `save_var`, `clear_vars`, `delete` and `execute` methods automatically commit changes.
You can delay a series of such operations from commiting until all have completed as a group.

Do this by passing `commit=False` to each method.
But always make the transaction permanent at the end by committing manually with the `Store.commit` method.  For example:

    st = Store("dbtruck.db")
    st.insert({"name":"Bagger 293","manufacturer":"TAKRAF","height":95}, commit=False)
    st.save_var('page_number', 42, commit=False)
    # note you can test for errors and do st.rollback() here to undo both above operations
    st.commit() # both updates made permanent here
    
### Max, min and count
You can get a maximum and minimum value for a particular `field` and an overall count or a count based on some conditions

    max_weight = st.get_max(field='weight', table_name='tools')
    min_weight = st.get_min(field='weight', table_name='tools')
    num_blue_tools = st.count(conditions='colour = ?', params='blue', table_name='tools')
    
### Other functions
Miscellaneous useful functions

    st.dump_to(filename, table_name='tools') # dumps a table to a CSV file
    st.load_from(filename, table_name='tools') # loads a table from a CSV file
    st.vacuum() # compresses the database (note has no effect in SQL Server)
    st.close() # closes the connection to the database

### I/O details of Python data types
* `bool` - normally returned as `True` or `False`, but if you specify `bool_int_output`, values are returned as 1 or 0.
        When setting up select conditions using a boolean field pay attention to the SQL syntax - 
        in MySQL, PostgreSQL and SQlite it is best to test for an implicit true and implicit false
        ie `WHERE bool_field` and `WHERE NOT bool_field` will work across all three underlying databases (whereas `bool_field <> 0` or `bool_field = 'false'` will depend on the
        underlying database syntax).
        In SQL Server, implicit true/false does not work, you should use `WHERE bool_field=0` (false) and `WHERE bool_field=1` (true) as your tests.
* `bytearray` - stored as bytes (and converted back on output)
* `bytes` - binary bytes
* `datetime.date` - date objects and ISO8601 format strings are accepted as input
        If you specify `dates_str_output` dates  are output as ISO8601 strings
* `datetime.datetime` - datetime objects and ISO8601 format strings are are accepted as input. 
        If you specify `dates_str_output` datetimes are output in Python ISO8601 format (with a 'T' separator not a space)
* `datetime.time` - time objects and ISO8601 format strings are accepted as input. 
        (in SQL Server, note that any microseconds are stripped from the stored value).
        If you specify `dates_str_output` times are output as ISO8601 strings
* `dict` - stored as a JSON string (which means the keys will always be converted to strings when output), but normally
        returned as a `dict`, unless you specify `json_str_output`.
* `float` - floating point numbers
* `int` - integer numbers
* `list` - stored as a JSON string but converted back on output, unless you specify `json_str_output`.
* `set` - stored as a JSON list string but converted back on output, unless you specify `json_str_output`.
* `str` - unicode text
* `tuple` - stored as a JSON list string but converted back on output, unless you specify `json_str_output`.


### SQL convenience functions
The following Store methods return appropriately formatted and quoted SQL fragments 
that can be inserted directly into the 'conditions' parameter described above or into `Store.execute`.

If supplied with Python date or date time objects or ISO8601 strings, they return a quoted literal. If supplied with the name of a date field 
from the Store they will use that appropriately instead.

* sql_dtm (this_dtm) - returns a date time expression
* sql_dt (this_dt) - returns a date expression
* sql_dt_inc (this_dt, inc) - an expression for a date incremented by an integer number of days (negative or positive)
* sql_before_dt (ref_dt, cmp_dt) - a statement which is true if the reference date is before the comparison date
* sql_after_dt (ref_dt, cmp_dt) - a statement which is true if the reference date is after the comparison date



