import dir_ops as do
import py_starter as ps
import os
import database_connections.Query as Query
import database_connections.Connections as Connections
import importlib 


def get_DatabaseConnection( connection_module = 'sqlite', **kwargs ):

    '''returns a class object from the given SQL connection module'''

    Conns_obj = importlib.__import__( 'database_connections.Connections', fromlist=[connection_module] )
    module = getattr( Conns_obj, connection_module )
    conn = module.get_DatabaseConnection( **kwargs )
    return conn

def list_to_string( iterable, sep = ',', quotes = "'" ):

    """Given a list [ 'meter1', 'meter2', 'meter3' ] spits out  '1234','2345','3456'"""

    item_separator = quotes + sep + quotes
    string = quotes + item_separator.join( iterable ) + quotes

    return string

def run_queries_in_folder( queries_Dir, export_Dir, conn_inst, print_df: bool = True, export_type = 'csv', parquet_engine = 'fastparquet', file_endings = ['sql'] ):

    """Given a Dir of Queries, an Export Dir, and a database connection, run all queries as specified by the user"""

    sql_Paths = queries_Dir.list_contents_Paths( block_dirs = True )
    
    valid_Paths = do.Paths()
    for sql_Path in sql_Paths:
        if sql_Path.ending in file_endings:
            valid_Paths._add( sql_Path )

    to_run_Paths = ps.get_selections_from_list( valid_Paths )

    for sql_Path in to_run_Paths:

        export_Path = export_Dir.join_Path( path = sql_Path.root + '.' + export_type ) 

        export_kwargs = {}
        if export_Path.ending == 'parquet':
            export_kwargs['engine'] = parquet_engine

        sql_Query = Query.Query( query_Path = sql_Path, export_Path = export_Path, conn_inst = conn_inst )
        sql_Query.query()
        sql_Query.export( **export_kwargs )

        if print_df:
            sql_Query.df

