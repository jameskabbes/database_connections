import sqlalchemy
from database_connections.DatabaseConnection import DatabaseConnection

def get_DatabaseConnection( **kwargs ):

    '''returns the class instance of the said object'''
    return Databricks( **kwargs )

class Databricks( DatabaseConnection ):

    '''To run the Databricks module, you need to set the following attributes:

    server_hostname
    http_path
    access_token
    schema

    '''

    def __init__( self, **kwargs ):

        DatabaseConnection.__init__( self, **kwargs)

    def init( self, **kwargs ):

        self.set_atts( kwargs )
        self.get_conn()
        # skip getting cursor since Databricks doesn't have one

    def get_conn( self, **kwargs ):

        self.conn = sqlalchemy.create_engine(
            "databricks+connector://token:{access_token}@{server_hostname}:443/{schema}".format(
                access_token = self.access_token, server_hostname = self.server_hostname, schema = self.schema ),
            connect_args={"http_path": self.http_path}
        )


