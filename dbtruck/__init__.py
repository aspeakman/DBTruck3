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
from .dbtruck import Store
from .adapcast import Pickle, ISODate, ISODateTime, ISOTime