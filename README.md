Script to provide a bloat report for PostgreSQL tables and/or indexes. Requires the pgstattuple contrib module - https://www.postgresql.org/docs/current/static/pgstattuple.html

Note that the query to check for bloat can be extremely expensive on very large databases or those with many tables. The script first requires running --create_stats_table to create a table for storing the bloat statistics. This makes it easier for reviewing the bloat statistics or running a regular monitoring interval without having to rescan the database again.

Output formats are a simple text listing, ordered by wasted space. Or a python dictionary that provides more detail and can be used by other python scripts or tools that need a more structured format.

Filters are available for bloat percentage, wasted size and object size. The wasted size reported is a combination of dead tuples and free space in the given object. The wasted size value will likely never be zero (unless the object is completely empty) since most tables keep a small amount of free space available for themselves. Use the dictionary output to see the distinction between dead tuples and free space for a more accurate picture of the bloat situation. If dead tuples is high, this means autovacuum is likely not able to run frequently enough on the given table. If dead tuples is low but free space is high, this indicates a vacuum full is likely required to clear the bloat and return the disk space to the system.

See --help for more information.

NOTE: The 1.x version of this script used the bloat query found in check_postgres.pl. While that query runs much faster than using pgstattuple, it can be inaccurate at times missing large amounts of table and index bloat. Using pgstattuple provides the best method known to obtain the most accurate bloat statistics.
