Script to provide a bloat report for PostgreSQL tables and/or indexes. 

Filters are available for bloat percentage, page size & wasted size.

Output formats are a simple text listing, ordered by wasted space. Or a python dictionary that provides more detail and can be used by other python scripts or tools that need a more structured format.

Note that the query to check for bloat can be extremely expensive on very large databases or those with many tables. The materialized view option in 9.3+ can help to mitigate getting bloat data back by storing it persistently instead of running a query every time the script is run.

