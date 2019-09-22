# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    # THIS IS A LIST OF ORDERED DICTIONARIES
    csv_tbl = CSVDataTable("people", connect_info, ['playerID'])

    print("Created table = " + str(csv_tbl))
    # print(csv_tbl._rows[0])
    # print(csv_tbl._rows[-1])
    print(csv_tbl.find_by_primary_key(['aaronto01'], ['birthYear', 'birthState']))

    '''
    All methods:
    Assume call will fail, return information if it succeeds
        - Purpose: don't overlook points of failure assuming that grading will use pretty cases
    Handle logging from test cases, don't clutter class implementation with assignment-specific fluff
    Test cases:
    CSV: Parks.csv
    1) find_by_primary_key(
    2) find_by_template(
    3) delete_by_key(
    4) delete_by_template(
    5) update_by_key(
    6) update_by_template(
    7) insert(
    RDB: Schools.csv
    8) find_by_primary_key(
    9) find_by_template()
    10) delete_by_key((schoolID))
    11) delete_by_template(
    12) update_by_key([schoolID], (sanign1, San Ignactio de Loyola, San Juan, ***, Puerto Rico))
    13) update_by_template(
    14) insert((schoolID, name_full, city, state, country)
    '''


t_load()
