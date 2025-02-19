
# You can override any of these settings in local_test_settings.py - do not change them here

#CONNECT_STRING = "postgresql://user:password@localhost:5432/test"
#CONNECT_STRING = "mysql://user:password@127.0.0.1:3306/test"
CONNECT_STRING = "test_dbs" # SQLite is the default, a separate database file is created for each test (default folder = 'test_dbs')

SQLITE_T_SEPARATOR = False # if False, a space separates the date from the the time (in stored SQLite data fields only), otherwise a 'T'

try:
    from .local_test_settings import *
except ImportError:
    try:
        from local_test_settings import *
    except ImportError:
        pass