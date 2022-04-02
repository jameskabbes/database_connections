if __name__ == '__main__':
    import user_profile_import
    user_profile = user_profile_import.init()

import sqlalchemy
import psycopg2
import pandas as pd
import py_starter as ps
from DatabaseConnection import DatabaseConnection

def get_DatabaseConnection( **kwargs ):

    '''returns the class instance of the said object'''
    return Redshift( **kwargs )

class Redshift( DatabaseConnection ):

    '''To run the SQLite module, you need to set the following attributes:

    engine = 'postgresql+psycopg2://dfg_analtyics...'
    schema = 'dfg_analytics'

    '''

    def __init__( self, **kwargs ):

        DatabaseConnection.__init__( self, **kwargs)

    def init( self, **kwargs ):

        self.set_atts( kwargs )
        self.get_conn()
        # skip getting cursor since Redshift doesn't have one

    def get_conn(self):

        self.conn = sqlalchemy.create_engine( self.engine )

    def write(self, df, table_name, **kwargs):

        default_kwargs = {'schema': self.schema, 'if_exists': 'replace', 'index': False, 'method': 'multi'}
        kwargs = ps.replace_default_kwargs( default_kwargs, **kwargs )

        df.to_sql( table_name, self.conn, **kwargs )

    def create_generic_select( self, table_name, top = None ):

        '''returns a string with a generic select'''

        string = 'SELECT * FROM ' + str(table_name)

        if top != None:
            string += (' LIMIT ' + str(top) )

        return string

    def get_all_tables( self ):

        string = '''select t.table_name
        from information_schema.tables t
        where t.table_schema = '{schema}'
        '''.format( schema = self.schema  )

        df = self.query( query_string = string )
        return list(df['table_name'])
