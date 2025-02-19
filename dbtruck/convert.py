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
from copy import copy
import re
import datetime
from collections import OrderedDict

from .adapcast import Pickle

QUOTEPAIRS = [
  ('"', '"'),
  ('`', '`'),
  ('[', ']'),
]

IDENT_NOQUOTE = re.compile(r'^[a-z][a-z_0-9]*$')

def clean_data(data, remove_none=True, pickle_unknown=True):
    # Turns it into a list of lists of (key, value) tuples (+optional step to remove items with None values).

  try:
    data.keys
  except AttributeError:
    # http://stackoverflow.com/questions/1952464/
    # in-python-how-do-i-determine-if-a-variable-is-iterable
    try:
      [e for e in data]
    except TypeError:
      raise TypeError(
        'Data must be a mapping (like a dict), or an iterable.'
      )
  else:
    # It is a single dictionary
    data = [data]

  cleaned = []
  for row in data:

    checkdata(row)

    if remove_none:
      for key, value in list(row.items()):
        if value is None:
          del(row[key])
          

    if len(set([k.lower() for k in row.keys()])) != len(row.keys()):
      raise ValueError('You passed the same column name twice. (Column names are insensitive to case.)')

    newrow = zip(list(row.keys()), list(row.values())) 
    cleaned.append(list(newrow))
    
  return cleaned

def simplify(val):
  return re.sub(r'[^a-zA-Z0-9]', '', val)

def quote(val):
  'Handle quote characters'
  text = str(val)
  # Look for quote characters. Keep the text as is if it's already quoted.
  for qp in QUOTEPAIRS:
    if text[0] == qp[0] and text[-1] == qp[-1] and len(text) >= 2:
      return text

  # If it's not quoted, try quoting
  for qp in QUOTEPAIRS:
    if qp[1] not in text:
      return qp[0] + text + qp[1]

  #Darn
  raise ValueError('The value "%s" is not quoted and contains too many quote characters to quote' % text)
      
def iquote(text, force=False, qchar='"'): # quote identifiers
    """Quotes added if the string contains non-identifier characters or capital letters 
    Usually added only if necessary but can be forced
    Embedded quotes are properly doubled."""
    if force or not IDENT_NOQUOTE.match(text):
        if qchar in text:
            return qchar + text.replace(qchar, qchar + qchar) + qchar
        else:
            return qchar + text + qchar
    return text
    
def nquote(text): # quote nullable/literal
    """ Quote text as a literal; or, if the argument is null, return NULL.
    Embedded single-quotes and backslashes are properly doubled. """
    if text is None:
        return 'NULL'
    if '\\' in text: 
        text = text.replace('\\', '\\\\')
    if "'" in text:
        text = text.replace("'", "''")
    return "'%s'" % text

def checkdata(data):
  for key, value in data.items():
    # Column names
    if not key:
      raise ValueError('key must not be empty')
    elif not isinstance(key, str):
      raise ValueError('The column name must be a string. The column name ("%s") is of type %s.' % (key, type(key)))
    #elif isinstance(value, dict) and not all(isinstance(k, str) for k in value.keys()):
    #  raise ValueError('Dictionary keys must all be str type for database insert.')
