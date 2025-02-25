
# You can override any of these settings in local_test_settings.py - DO NOT CHANGE THEM HERE

#CONNECT_STRING = "postgresql://user:password@localhost:5432/test"
#CONNECT_STRING = "mysql://user:password@127.0.0.1:3306/test"
CONNECT_STRING = "test_dbs" # SQLite is the default, this the name of a folder, a separate database file is created for each test in the folder

SQLITE_T_SEPARATOR = False # if False, a space separates the date from the the time (in stored SQLite data fields only), otherwise a 'T'
USE_ROWIDS = False # if True stores and return rowids

try:
    from .local_test_settings import *
except ImportError:
    try:
        from local_test_settings import *
    except ImportError:
        pass