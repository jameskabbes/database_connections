import sys
sys_args = sys.argv[1:]

from database_connections.database_connections import run
run( *sys_args )

