if __name__ == '__main__':
    import user_profile_import
    user_profile = user_profile_import.init()

import sqlite3
import sql_support_functions as ssf
import pandas as pd
from dir_ops import Path

from DatabaseConnection import DatabaseConnection

def get_DatabaseConnection( **kwargs ):

    '''returns the class instance of the said object'''
    return SQLite( **kwargs )


class SQLite( DatabaseConnection ):

    '''To run the SQLite module, you need to set the following attributes:

    db_path = '/path/to/database.db'

    '''

    def __init__( self, **kwargs ):

        DatabaseConnection.__init__( self, **kwargs)

    def exit( self ):

        '''close the connection'''

        self.close_conn()

    def get_conn( self ):

        '''get the connection'''

        try:
            conn = sqlite3.connect( self.db_path )

        except:
            print ('Could not connect to SQLite Database: ' + str(self.db_path))
            conn = None

        self.conn = conn

    def close_conn( self ):

        '''close the connection'''
        self.conn.close()

    def get_all_tables( self ):

        '''returns a list of all available table names'''

        string = '''
        SELECT name FROM sqlite_master
        WHERE type IN ('table','view')
        AND name NOT LIKE 'sqlite_%'
        ORDER BY 1;'''

        df = self.query( query_string = string )
        return list( df['name'] )

    def create_generic_select( self, table_name, top = None ):

        '''returns a string with a generic select statement'''

        string = 'SELECT * FROM ' + str(table_name)

        if top != None:
            string += (' LIMIT ' + str(top) )

        return string


if __name__ == '__main__':

    db_path = 'test.db'
    conn_inst = SQLite( db_path = db_path )

    #Write to db
    df = pd.DataFrame( {'X':list(range(100)), 'Y':list(range(100))} )
    conn_inst.write( df, 'test' )

    # Read from db
    df = conn_inst.query( query_string = 'Select X FROM test WHERE X < 50' )
    print (df)

    conn_inst.exit()
