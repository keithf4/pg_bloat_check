#!/usr/bin/env python

# Script is maintained at https://github.com/keithf4/pg_bloat_check

import argparse, csv, json, psycopg2, re, sys
from psycopg2 import extras
from random import randint

version = "2.7.1"

parser = argparse.ArgumentParser(description="Provide a bloat report for PostgreSQL tables and/or indexes. This script uses the pgstattuple contrib module which must be installed first. Note that the query to check for bloat can be extremely expensive on very large databases or those with many tables. The script stores the bloat stats in a table so they can be queried again as needed without having to re-run the entire scan. The table contains a timestamp columns to show when it was obtained.")
args_general = parser.add_argument_group(title="General options")
args_general.add_argument('-c','--connection', default="host=", help="""Connection string for use by psycopg. Defaults to "host=" (local socket).""")
args_general.add_argument('-e', '--exclude_object_file', help="""Full path to file containing a list of objects to exclude from the report (tables and/or indexes). Each line is a CSV entry in the format: objectname,bytes_wasted,percent_wasted. All objects must be schema qualified. bytes_wasted & percent_wasted are additional filter values on top of -s, -p, and -z to exclude the given object unless these values are also exceeded. Set either of these values to zero (or leave them off entirely) to exclude the object no matter what its bloat level. Comments are allowed if the line is prepended with "#". See the README.md for clearer examples of how to use this for more fine grained filtering.""")
args_general.add_argument('-f', '--format', default="simple", choices=["simple", "json", "jsonpretty", "dict"], help="Output formats. Simple is a plaintext version suitable for any output (ex: console, pipe to email). Object type is in parentheses (t=table, i=index, p=primary key). Json provides standardized json output which may be useful if taking input into something that needs a more structured format. Json also provides more details about dead tuples, empty space & free space. jsonpretty outputs in a more human readable format. Dict is the same as json but in the form of a python dictionary. Default is simple.")
args_general.add_argument('-m', '--mode', choices=["tables", "indexes", "both"], default="both", help="""Provide bloat reports for tables, indexes or both. Index bloat is always distinct from table bloat and reported as separate entries in the report. Default is "both". NOTE: GIN indexes are not supported at this time and will be skipped.""")
args_general.add_argument('-n', '--schema', help="Comma separated list of schema to include in report. pg_catalog schema is always included. All other schemas will be ignored.")
args_general.add_argument('-N', '--exclude_schema', help="Comma separated list of schemas to exclude.")
args_general.add_argument('--noanalyze', action="store_true", help="To ensure accurate fillfactor statistics, an analyze if each object being scanned is done before the check for bloat. Set this to skip the analyze step and reduce overall runtime, however your bloat statistics may not be as accurate.")
args_general.add_argument('--noscan', action="store_true", help="Set this option to have the script just read from the bloat statistics table without doing a scan of any tables again.")
args_general.add_argument('-p', '--min_wasted_percentage', type=float, default=0.1, help="Minimum percentage of wasted space an object must have to be included in the report. Default and minimum value is 0.1 (DO NOT include percent sign in given value).")
args_general.add_argument('-q', '--quick', action="store_true", help="Use the pgstattuple_approx() function instead of pgstattuple() for a quicker, but possibly less accurate bloat report on tables. Note that this does not work on indexes or TOAST tables and those objects will continue to be scanned with pgstattuple() and still be included in the results. Sets the 'approximate' column in the bloat statistics table to True. Note this only works in PostgreSQL 9.5+.")
args_general.add_argument('-u', '--quiet', default=0, action="count", help="Suppress console output but still insert data into the bloat statistics table. This option can be set several times. Setting once will suppress all non-error console output if no bloat is found, but still output when it is found for given parameter settings. Setting it twice will suppress all console output, even if bloat is found.")
args_general.add_argument('-r', '--commit_rate', type=int, default=5, help="Sets how many tables are scanned before committing inserts into the bloat statistics table. Helps avoid long running transactions when scanning large tables. Default is 5. Set to 0 to avoid committing until all tables are scanned. NOTE: The bloat table is truncated on every run unless --noscan is set.")
args_general.add_argument('--rebuild_index', action="store_true", help="Output a series of SQL commands for each index that will rebuild it with minimal impact on database locks. This does NOT run the given sql, it only provides the commands to do so manually. This does not run a new scan and will use the indexes contained in the statistics table from the last run. If a unique index was previously defined as a constraint, it will be recreated as a unique index. All other filters used during a standard bloat check scan can be used with this option so you only get commands to run for objects relevant to your desired bloat thresholds.")
args_general.add_argument('--recovery_mode_norun', action="store_true", help="Setting this option will cause the script to check if the database it is running against is a replica (in recovery mode) and cause it to skip running. Otherwise if it is not in recovery, it will run as normal. This is useful for when you want to ensure the bloat check always runs only on the primary after failover without having to edit crontabs or similar process managers.")
args_general.add_argument('-s', '--min_size', default=1, help="Minimum size in bytes of object to scan (table or index). Default and minimum value is 1. Size units (mb, kb, tb, etc.) can be provided as well")
args_general.add_argument('-t', '--tablename', help="Scan for bloat only on the given table. Must be schema qualified. This always gets both table and index bloat and overrides all other filter options so you always get the bloat statistics for the table no matter what they are.")
args_general.add_argument('--version', action="store_true", help="Print version of this script.")
args_general.add_argument('-z', '--min_wasted_size', default=1, help="Minimum size of wasted space in bytes. Default and minimum is 1. Size units (mb, kb, tb, etc.) can be provided as well")
args_general.add_argument('--debug', action="store_true", help="Output additional debugging information. Overrides quiet option.")

args_setup = parser.add_argument_group(title="Setup")
args_setup.add_argument('--pgstattuple_schema', help="If pgstattuple is not installed in the default search path, use this option to designate the schema where it is installed.")
args_setup.add_argument('--bloat_schema', help="Set the schema that the bloat report table is in if it's not in the default search path. Note this option can also be set when running --create_stats_table to set which schema you want the table created.")
args_setup.add_argument('--create_stats_table', action="store_true", help="Create the required tables that the bloat report uses (bloat_stats + two child tables). Places table in default search path unless --bloat_schema is set.")
args = parser.parse_args()


def check_pgstattuple(conn):
    sql = "SELECT e.extversion, n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid WHERE extname = 'pgstattuple'"
    cur = conn.cursor()
    cur.execute(sql)
    pgstattuple_info = cur.fetchone()
    if pgstattuple_info == None:
        print("pgstattuple extension not found. Please ensure it is installed in the database this script is connecting to.")
        close_conn(conn)
        sys.exit(2)
    if args.pgstattuple_schema != None:
        if args.pgstattuple_schema != pgstattuple_info[1]:
            print("pgstattuple not found in the schema given by --pgstattuple_schema option: " + args.pgstattuple_schema + ". Found instead in: " + pgstattuple_info[1]+".")
            close_conn(conn)
            sys.exit(2)
    return pgstattuple_info[0]


def check_recovery_status(conn):
    sql = "SELECT pg_is_in_recovery FROM pg_catalog.pg_is_in_recovery()"
    cur = conn.cursor()
    cur.execute(sql)
    is_in_recovery = cur.fetchone()[0]
    cur.close()
    return is_in_recovery


def create_conn():
    conn = psycopg2.connect(args.connection)
    return conn


def close_conn(conn):
    conn.close()


def create_list(list_type, list_items):
    split_list = []
    if list_type == "csv":
        split_list = list_items.split(',')
    elif list_type == "file":
        with open(list_items, 'r') as csvfile:
            objectreader = csv.DictReader(csvfile, fieldnames=['objectname', 'max_wasted', 'max_perc'])
            for o in objectreader:
                if not o['objectname'].startswith('#'):
                    o['objectname'] = o['objectname'].strip()

                    if o['max_wasted'] != None:
                        o['max_wasted'] = int(o['max_wasted'])
                    else:
                        o['max_wasted'] = 0

                    if o['max_perc'] != None:
                        o['max_perc'] = float(o['max_perc'])
                    else:
                        o['max_perc'] = 0

                    split_list.append(o)

    return split_list


def create_stats_table(conn):
    if args.bloat_schema != None:
        parent_sql = args.bloat_schema + "." + "bloat_stats"
        tables_sql = args.bloat_schema + "." + "bloat_tables"
        indexes_sql = args.bloat_schema + "." + "bloat_indexes"
    else:
        parent_sql = "bloat_stats"
        tables_sql = "bloat_tables"
        indexes_sql = "bloat_indexes"

    drop_sql = "DROP TABLE IF EXISTS " + parent_sql + " CASCADE"

    sql = "CREATE TABLE " + parent_sql + """ (
                              oid oid NOT NULL
                            , schemaname text NOT NULL
                            , objectname text NOT NULL
                            , objecttype text NOT NULL
                            , size_bytes bigint
                            , live_tuple_count bigint
                            , live_tuple_percent float8
                            , dead_tuple_count bigint
                            , dead_tuple_size_bytes bigint
                            , dead_tuple_percent float8
                            , free_space_bytes bigint
                            , free_percent float8
                            , stats_timestamp timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
                            , approximate boolean NOT NULL DEFAULT false
                            , relpages bigint NOT NULL DEFAULT 1
                            , fillfactor float8 NOT NULL DEFAULT 100)"""
    cur = conn.cursor()
    if args.debug:
        print(cur.mogrify("drop_sql: " + drop_sql))
    cur.execute(drop_sql)
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)
    sql = "CREATE TABLE " + tables_sql + " (LIKE " + parent_sql + " INCLUDING ALL) INHERITS (" + parent_sql + ")"
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)
    sql = "CREATE TABLE " + indexes_sql + " (LIKE " + parent_sql + " INCLUDING ALL) INHERITS (" + parent_sql + ")"
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)
    sql = "COMMENT ON TABLE " + parent_sql + " IS 'Table providing raw data for table & index bloat'"
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)
    sql = "COMMENT ON TABLE " + tables_sql + " IS 'Table providing raw data for table bloat'"
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)
    sql = "COMMENT ON TABLE " + indexes_sql + " IS 'Table providing raw data for index bloat'"
    if args.debug:
        print(cur.mogrify("sql: " + sql))
    cur.execute(sql)

    conn.commit()
    cur.close()


def get_bloat(conn, exclude_schema_list, include_schema_list, exclude_object_list):
    sql = ""
    commit_counter = 0
    analyzed_tables = []
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = "SELECT current_setting('block_size')"
    cur.execute(sql)
    block_size = int(cur.fetchone()[0])

    sql_tables = """ SELECT c.oid, c.relkind, c.relname, n.nspname, 'false' as indisprimary, c.reloptions
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE relkind IN ('r', 'm')
                    AND c.relpersistence <> 't' """

    sql_indexes = """ SELECT c.oid, c.relkind, c.relname, n.nspname, i.indisprimary, c.reloptions 
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid
                    JOIN pg_catalog.pg_am a ON c.relam = a.oid
                    WHERE c.relkind = 'i' 
                    AND c.relpersistence <> 't'
                    AND a.amname <> 'gin' AND a.amname <> 'brin' """


    cur.execute("SELECT current_setting('server_version_num')::int >= 90300")
    if cur.fetchone()[0] == True:
        sql_indexes += " AND indislive = 'true' "

    if args.tablename != None:
        sql_tables += " AND n.nspname||'.'||c.relname = %s "
        sql_indexes += " AND i.indrelid::regclass = %s::regclass "

        sql_class = sql_tables + """
                    UNION 
                    """ + sql_indexes

        if args.debug:
            print("sql_class: " + str(cur.mogrify(sql_class, [args.tablename, args.tablename])) )
        cur.execute(sql_class, [args.tablename, args.tablename] )
    else:
        # IN clauses work with python tuples. lists were converted by get_bloat() call
        if include_schema_list:
            sql_tables += " AND n.nspname IN %s"
            sql_indexes += " AND n.nspname IN %s"
            filter_list = include_schema_list
        elif exclude_schema_list:
            sql_tables += " AND n.nspname NOT IN %s"
            sql_indexes += " AND n.nspname NOT IN %s"
            filter_list = exclude_schema_list
        else:
            filter_list = ""

        if args.mode == 'tables':
            sql_class = sql_tables
        elif args.mode == 'indexes':
            sql_class = sql_indexes
        elif args.mode == "both":
            sql_class = sql_tables + """
                    UNION 
                    """ + sql_indexes

        if args.mode == "both":
            if args.debug:
                print("sql_class: " + str(cur.mogrify(sql_class, (filter_list,filter_list) )) )
            cur.execute(sql_class, (filter_list,filter_list))
        elif args.mode == "tables" or args.mode == "indexes":
            if args.debug:
                print("sql_class: " + str(cur.mogrify(sql_class, (filter_list,) )) )
            cur.execute(sql_class, (filter_list,) )
        else:
            cur.execute(sql)

    object_list_no_toast = cur.fetchall()

    # Gather associated toast tables after generating above list so that only toast tables relevant
    # to either schema or table filtering are gathered
    object_list_with_toast = []
    for o in object_list_no_toast:
        # Add existing objects to new list
        object_list_with_toast.append(o)

        if o['relkind'] == 'r' or o['relkind'] == 'm':
            # only run for tables or mat views to speed things up
            # Note tables without a toast table have the value 0 for reltoastrelid, not NULL
            sql_toast = """  WITH toast_data AS (
                            SELECT reltoastrelid FROM pg_class WHERE oid = %s AND reltoastrelid != 0
                        )
                        SELECT c.oid, c.relkind, c.relname, n.nspname, 'false' as indisprimary, c.reloptions
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                        JOIN toast_data ON c.oid = toast_data.reltoastrelid
                        AND c.relpersistence <> 't' """
            cur.execute(sql_toast, [ o['oid'] ])
            # Add new toast table to list if one exists for given table
            result = cur.fetchone()
            if result != None: 
                object_list_with_toast.append(result)

    if args.debug:
        for o in object_list_with_toast:
            print("object_list_with_toast: " + str(o))

    sql = "TRUNCATE "
    if args.bloat_schema:
        sql += args.bloat_schema + "."
    if args.mode == "tables" or args.mode == "both":
        sql_table = sql + "bloat_tables"
        cur.execute(sql_table)
    if args.mode == "indexes" or args.mode == "both":
        sql_index = sql + "bloat_indexes"
        cur.execute(sql_index)
    conn.commit()

    for o in object_list_with_toast:
        if args.debug:
            print("begining of object list loop: " + str(o))
        if exclude_object_list and args.tablename == None:
            # completely skip object being scanned if it's in the excluded file list with max values equal to zero
            match_found = False
            for e in exclude_object_list:
                if (e['objectname'] == o['nspname'] + "." + o['relname']) and (e['max_wasted'] == 0) and (e['max_perc'] == 0):
                    match_found = True
            if match_found:
                continue

        if o['relkind'] == "i": 
            fillfactor = 90.0
        else:
            fillfactor = 100.0

        if o['reloptions'] != None:
            reloptions_dict = dict(o.split('=') for o in o['reloptions'])
            if 'fillfactor' in reloptions_dict:
                fillfactor = float(reloptions_dict['fillfactor'])
        
        sql = """ SELECT count(*) FROM pg_catalog.pg_class WHERE oid = %s """
        cur.execute(sql, [ o['oid'] ])
        exists = cur.fetchone()[0]
        if args.debug:
            print("Checking for table existence before scanning: " + str(exists))
        if exists == 0:
            continue  # just skip over it. object was dropped since initial list was made

        if args.noanalyze != True:
            if o['relkind'] == "r" or o['relkind'] == "m" or o['relkind'] == "t":
                quoted_table = "\"" + o['nspname'] + "\".\"" + o['relname'] + "\""
            else:
                # get table that index is a part of
                sql = """SELECT n.nspname, c.relname
                            FROM pg_catalog.pg_class c 
                            JOIN pg_catalog.pg_index i ON c.oid = i.indrelid 
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
                            WHERE indexrelid = %s"""
                cur.execute(sql, [ o['oid'] ] )
                result = cur.fetchone()
                quoted_table = "\"" + result[0] + "\".\"" + result[1] + "\""

            # maintain a list of analyzed tables so that if a table was already analyzed, it's not again (ex. multiple indexes on same table)
            if quoted_table in analyzed_tables:
                if args.debug:
                    print("Table already analyzed. Skipping...")
                pass
            else:
                sql = "ANALYZE " + quoted_table
                if args.debug:
                    print(cur.mogrify(sql, [quoted_table]))
                cur.execute(sql)
                analyzed_tables.append(quoted_table)
        # end noanalyze check

        sql = """ SELECT c.relpages FROM pg_catalog.pg_class c 
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
                    WHERE n.nspname = %s
                    AND c.relname = %s """
        cur.execute(sql, [o['nspname'], o['relname']])
        relpages = int(cur.fetchone()[0])

        if args.quick and (o['relkind'] == "r" or o['relkind'] == "m"):
            # pgstattuple_approx() does not work against toast tables
            approximate = True
            sql = "SELECT table_len, approx_tuple_count AS tuple_count, approx_tuple_len AS tuple_len, approx_tuple_percent AS tuple_percent, dead_tuple_count,  "
            sql += "dead_tuple_len, dead_tuple_percent, approx_free_space AS free_space, approx_free_percent AS free_percent FROM "
        else:
            approximate = False
            sql = "SELECT table_len, tuple_count, tuple_len, tuple_percent, dead_tuple_count, dead_tuple_len, dead_tuple_percent, free_space, free_percent FROM "
        if args.pgstattuple_schema != None:
            sql += " \"" + args.pgstattuple_schema + "\"."
        if args.quick and (o['relkind'] == "r" or o['relkind'] == "m"):
            sql += "pgstattuple_approx(%s::regclass) "
            if args.tablename == None:
                sql += " WHERE table_len > %s"
                sql += " AND ( (dead_tuple_len + approx_free_space) > %s OR (dead_tuple_percent + approx_free_percent) > %s )"
        else:
            sql += "pgstattuple(%s::regclass) "
            if args.tablename == None:
                sql += " WHERE table_len > %s"
                sql += " AND ( (dead_tuple_len + free_space) > %s OR (dead_tuple_percent + free_percent) > %s )"

        if args.tablename == None:
            if args.debug:
                print("sql: " + str(cur.mogrify(sql, [ o['oid']
                                                    , convert_to_bytes(args.min_size)
                                                    , convert_to_bytes(args.min_wasted_size)
                                                    , args.min_wasted_percentage])) )
            cur.execute(sql, [ o['oid']
                                , convert_to_bytes(args.min_size)
                                , convert_to_bytes(args.min_wasted_size)
                                , args.min_wasted_percentage ])
        else:
            if args.debug:
                print("sql: " + str(cur.mogrify(sql, [ o['oid'] ])) )
            cur.execute(sql, [ o['oid'] ])

        stats = cur.fetchall()

        if args.debug:
            print(stats)

        if stats: # completely empty objects will be zero for all stats, so this would be an empty set

            # determine byte size of fillfactor pages 
            ff_relpages_size = (relpages - ( fillfactor/100 * relpages ) ) * block_size

            if exclude_object_list and args.tablename == None:
                # If object in the exclude list has max values, compare them to see if it should be left out of report
                wasted_space = stats[0]['dead_tuple_len'] + (stats[0]['free_space'] - ff_relpages_size)
                wasted_perc = stats[0]['dead_tuple_percent'] + (stats[0]['free_percent'] - (100-fillfactor))
                for e in exclude_object_list:
                    if (e['objectname'] == o['nspname'] + "." + o['relname']):
                        if ( (e['max_wasted'] < wasted_space ) or (e['max_perc'] < wasted_perc ) ):
                            match_found = False
                        else:
                            match_found = True
                if match_found:
                    continue

            sql = "INSERT INTO "
            if args.bloat_schema != None:
                sql += args.bloat_schema + "."

            if o['relkind'] == "r" or o['relkind'] == "m" or o['relkind'] == "t":
                sql+= "bloat_tables"
                if o['relkind'] == "r":
                    objecttype = "table"
                elif o['relkind'] == "t":
                    objecttype = "toast_table"
                else:
                    objecttype = "materialized_view"
            elif o['relkind'] == "i":
                sql+= "bloat_indexes"
                if o['indisprimary'] == True:
                    objecttype = "index_pk"
                else:
                    objecttype = "index"
                
            sql += """ (oid
                        , schemaname
                        , objectname 
                        , objecttype
                        , size_bytes
                        , live_tuple_count
                        , live_tuple_percent
                        , dead_tuple_count
                        , dead_tuple_size_bytes
                        , dead_tuple_percent
                        , free_space_bytes
                        , free_percent
                        , approximate
                        , relpages
                        , fillfactor)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            if args.debug:
                print("insert sql: " + str(cur.mogrify(sql, [ o['oid']
                                                            , o['nspname']
                                                            , o['relname']
                                                            , objecttype 
                                                            , stats[0]['table_len']
                                                            , stats[0]['tuple_count']
                                                            , stats[0]['tuple_percent']
                                                            , stats[0]['dead_tuple_count']
                                                            , stats[0]['dead_tuple_len']
                                                            , stats[0]['dead_tuple_percent']
                                                            , stats[0]['free_space']
                                                            , stats[0]['free_percent']
                                                            , approximate
                                                            , relpages
                                                            , fillfactor
                                                        ])) ) 
            cur.execute(sql, [   o['oid'] 
                               , o['nspname']
                               , o['relname']
                               , objecttype
                               , stats[0]['table_len']
                               , stats[0]['tuple_count']
                               , stats[0]['tuple_percent']
                               , stats[0]['dead_tuple_count']
                               , stats[0]['dead_tuple_len']
                               , stats[0]['dead_tuple_percent']
                               , stats[0]['free_space']
                               , stats[0]['free_percent']
                               , approximate
                               , relpages
                               , fillfactor
                             ]) 

        commit_counter += 1
        if args.commit_rate > 0 and (commit_counter % args.commit_rate == 0):
            if args.debug:
                print("Batch committed. Object scanned count: " + str(commit_counter))
            conn.commit()
    conn.commit()
    cur.close()
## end get_bloat()            


def print_report(result_list):
    if args.format == "simple":
        for r in result_list:
            print(r)
    else:
        print(result_list)


def print_version():
    print("Version: " + version)


def rebuild_index(conn, index_list):

    if index_list == []:
        print("Bloat statistics table contains no indexes for conditions given.")
        close_conn(conn)
        sys.exit(0)
    
    for i in index_list:
        temp_index_name = "pgbloatcheck_rebuild_" + str(randint(1000,9999))
        quoted_index = "\"" + i['schemaname'] + "\".\"" + i['objectname'] + "\""
        # get table index is in
        sql = """SELECT n.nspname, c.relname, t.spcname
                    FROM pg_catalog.pg_class c 
                    JOIN pg_catalog.pg_index i ON c.oid = i.indrelid 
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
                    LEFT OUTER JOIN pg_catalog.pg_tablespace t ON c.reltablespace = t.oid
                    WHERE indexrelid = %s"""
        cur.execute(sql, [ i['oid'] ] )
        result = cur.fetchone()
        quoted_table = "\"" + result[0] + "\".\"" + result[1] + "\""
        if result[2] != None:
            quoted_tablespace = "\"" + result[2] + "\""
        else:
            quoted_tablespace = None
        # create temp index definition
        sql = "SELECT pg_get_indexdef(%s::regclass)"
        cur.execute(sql, [ "\"" + i['schemaname'] +"\".\""+ i['objectname'] + "\"" ])
        index_def = cur.fetchone()[0]
        index_def = re.sub(r' INDEX', ' INDEX CONCURRENTLY', index_def, 1)
        index_def = index_def.replace(i['objectname'], temp_index_name, 1)
        if quoted_tablespace != None:
            index_def += " TABLESPACE " + quoted_tablespace
        index_def += ";"
        # check if index is clustered
        sql = "SELECT indisclustered FROM pg_catalog.pg_index WHERE indexrelid = %s"
        cur.execute(sql, [ i['oid'] ])
        indisclustered = cur.fetchone()[0]
        # start output
        print("")
        print(index_def)
        if indisclustered == True:
            print("ALTER TABLE " + quoted_table + " CLUSTER ON " + temp_index_name) + ";"
        # analyze table
        print("ANALYZE " + quoted_table + ";")
        if i['objecttype'] == "index":
            # drop old index or unique constraint
            sql = "SELECT count(*) FROM pg_catalog.pg_constraint WHERE conindid = %s"
            cur.execute(sql, [ i['oid'] ])
            isconstraint = int(cur.fetchone()[0])
            if isconstraint == 1:
                print("ALTER TABLE " + quoted_table + " DROP CONSTRAINT " + "\"" + i['objectname'] + "\";")
            else:
                print("DROP INDEX CONCURRENTLY " + quoted_index + ";")
            # analyze again
            print("ANALYZE " + quoted_table + ";")
            # rename temp index to original name
            print("ALTER INDEX \"" + i['schemaname'] + "\"." + temp_index_name + " RENAME TO \"" + i['objectname'] + "\";")
        elif i['objecttype'] == "index_pk":
            print("ALTER TABLE " + quoted_table + " DROP CONSTRAINT " + "\"" + i['objectname'] + "\";")
            # analyze again
            print("ANALYZE " + quoted_table + ";")
            print("ALTER TABLE " + quoted_table + " ADD CONSTRAINT " + i['objectname'] + " PRIMARY KEY USING INDEX " + temp_index_name + ";")
            # analyze again
            print("ANALYZE " + quoted_table + ";")
        if indisclustered == True:
            print("")
            print("-- WARNING: The following statement will exclusively lock the table for the duration of its runtime.")
            print("--   Uncomment it or manually run it to recluster the table on the newly created index.")
            print("-- CLUSTER " + quoted_table + ";")

        print("")
# end rebuild_index

def convert_to_bytes(val):

   # immediately return if val is a number
   # (i.e., no units were provided)
   if isinstance(val, int):
       return val
   if isinstance(val, str) and val.isdigit():
       return int(val)

   # split numbers from unit descriptor
   # we assume format is "####kb" or "####MB" etc.
   # we assume there are no spaces between number
   # and units
   match = re.search(r'^([0-9]+)([a-zA-Z]+)?',val)

   if match:
     num = int(match.group(1))
     unit = match.group(2)

     if args.debug:
       print("arg was broken into " + str(num) + " and " + unit)

     # we shouldn't get here (because if we did,
     # it would be all numbers, and that case is
     # handled up at the top of this function),
     # but if we somehow manage to get here,
     # return num as bytes
     if unit is None:
       return num

     # just get the first letter of unit descriptor
     # lowercase the unit for multiplier lookup
     unit_prefix = unit[0].lower()

     # map 
     multiplier={
              'b':0,
              'k':1,
              'm':2,
              'g':3,
              't':4,
              'p':5,
              'e':6,
              'z':7
              }
      
     # if the size descriptor cannot be interpreted, then ignore it
     exponent = multiplier.get(unit_prefix,0)

     if args.debug:
       print("multiplier calculated: 1024 ** " + str(exponent))

     return_bytes = num * (1024 ** exponent)

     if args.debug:
       print("calculated bytes: " + str(return_bytes))

     return return_bytes

   else:
     # return val if we don't know what to do with it
     return val
# end convert_to_bytes


if __name__ == "__main__":
    if args.version:
        print_version()
        sys.exit(0)

    if args.schema != None and args.exclude_schema != None:
        print("--schema and --exclude_schema are exclusive options and cannot be set together")
        sys.exit(2)

    if args.debug:
        print("quiet level: " + str(args.quiet))

    conn = create_conn()

    if args.recovery_mode_norun == True:
        is_in_recovery = check_recovery_status(conn)
        if is_in_recovery == True:
            if args.debug:
                print("Recovery mode check found instance in recovery. Skipping run.")
            close_conn(conn)
            sys.exit(0)
        else:
            if args.debug:
                print("Recovery mode check found primary instance. Running as normal.")

    pgstattuple_version = float(check_pgstattuple(conn))
    if args.quick:
        if pgstattuple_version < 1.3:
            print("--quick option requires pgstattuple version 1.3 or greater (PostgreSQL 9.5)")
            close_conn(conn)
            sys.exit(2)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if args.create_stats_table:
        create_stats_table(conn)
        close_conn(conn)
        sys.exit(0)

    sql = "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = %s"
    if args.bloat_schema != None:
        sql += " AND schemaname = %s"
        cur.execute(sql, ['bloat_stats', args.bloat_schema])
    else:
        cur.execute(sql, ['bloat_stats'])
    table_exists = cur.fetchone()
    if table_exists == None:
        print("Required statistics table does not exist. Please run --create_stats_table first before running a bloat scan.")
        close_conn(conn)
        sys.exit(2)

    if args.exclude_schema != None:
        exclude_schema_list = create_list('csv', args.exclude_schema)
    else:
        exclude_schema_list = []

    if args.schema != None:
        include_schema_list = create_list('csv', args.schema)
        # Always include pg_catalog even when user specifies their own list
        include_schema_list.append('pg_catalog')
    else:
        include_schema_list = []

    if args.exclude_object_file != None:
        exclude_object_list = create_list('file', args.exclude_object_file)
    else:
        exclude_object_list = []

    if args.noscan == False and args.rebuild_index == False:
        get_bloat(conn, tuple(exclude_schema_list), tuple(include_schema_list), exclude_object_list)

    # Final commit to ensure transaction that inserted stats data closes
    conn.commit()

    counter = 1
    result_list = []
    if args.quiet <= 1 or args.debug == True:
        simple_cols = """oid
                         , schemaname
                         , objectname
                         , objecttype
                         , CASE 
                            WHEN (dead_tuple_percent + (free_percent - (100-fillfactor))) < 0 THEN 0
                            ELSE (dead_tuple_percent + (free_percent - (100-fillfactor)))
                           END AS total_waste_percent
                         , CASE
                            WHEN (dead_tuple_size_bytes + (free_space_bytes - (relpages - (fillfactor/100) * relpages ) * current_setting('block_size')::int ) ) < 0 THEN '0 bytes'
                            ELSE pg_size_pretty((dead_tuple_size_bytes + (free_space_bytes - ((relpages - (fillfactor/100) * relpages ) * current_setting('block_size')::int ) ) )::bigint)
                           END AS total_wasted_size"""
        dict_cols = "oid, schemaname, objectname, objecttype, size_bytes, live_tuple_count, live_tuple_percent, dead_tuple_count, dead_tuple_size_bytes, dead_tuple_percent, free_space_bytes, free_percent, approximate, relpages, fillfactor"
        if args.format == "dict" or args.format=="json" or args.format=="jsonpretty" or args.rebuild_index:
            # Since "simple" is the default, this check needs to be first so that if args.rebuild_index is set, the proper columns are chosen
            sql = "SELECT " + dict_cols + " FROM "
        elif args.format == "simple":
            sql = "SELECT " + simple_cols + " FROM "
        else:
            print("Unsupported --format given. Use 'simple', 'dict' 'json', or 'jsonpretty'.")
            close_conn(conn)
            sys.exit(2)
        if args.bloat_schema != None:
            sql += args.bloat_schema + "."
        if args.mode == "tables":
            sql += "bloat_tables"
        elif args.mode == "indexes" or args.rebuild_index:
            sql += "bloat_indexes"
        else:
            sql += "bloat_stats"
        sql += " WHERE (dead_tuple_size_bytes + (free_space_bytes - (relpages - (fillfactor/100) * relpages ) * current_setting('block_size')::int ) ) > %s "
        sql += " AND (dead_tuple_percent + (free_percent - (100-fillfactor))) > %s "
        sql += " ORDER BY (dead_tuple_size_bytes + (free_space_bytes - ((relpages - (fillfactor/100) * relpages ) * current_setting('block_size')::int ) )) DESC"
        cur.execute(sql, [convert_to_bytes(args.min_wasted_size), args.min_wasted_percentage])
        result = cur.fetchall()

        # Output rebuild commands instead of status report
        if args.rebuild_index:
            rebuild_index(conn, result)
            close_conn(conn)
            sys.exit(0)

        for r in result:
            if args.format == "simple":
                if r['objecttype'] == 'table' or r['objecttype'] == 'toast_table':
                    type_label = 't'
                elif r['objecttype'] == 'index':
                    type_label = 'i'
                elif r['objecttype'] == 'index_pk':
                    type_label = 'p'
                elif r['objecttype'] == 'materialized_view':
                    type_label = 'mv'
                else:
                    print("Unexpected object type encountered in stats table. Please report this bug to author with value found: " + str(r['objecttype']))
                    sys.exit(2)

                justify_space = 100 - len(str(counter) + ". " + r['schemaname'] + "." + r['objectname'] + " (" + type_label + ") " + "(" + "{:.2f}".format(r['total_waste_percent']) + "%)" + r['total_wasted_size'] + " wasted")

                output_line = str(counter) + ". " + r['schemaname'] + "." + r['objectname'] + " (" + type_label + ") " + "."*justify_space + "(" + "{:.2f}".format(r['total_waste_percent']) + "%) " + r['total_wasted_size'] + " wasted"

                if r['objecttype'] == 'toast_table':
                    toast_real_sql = """ SELECT n.nspname||'.'||c.relname 
                                         FROM pg_catalog.pg_class c
                                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                         WHERE reltoastrelid = %s """
                    if args.debug:
                        print( "toast_real_sql: " + str(cur.mogrify(toast_real_sql, [r['oid']]) ) )
                    cur.execute(toast_real_sql, [r['oid']])
                    real_table = cur.fetchone()[0]
                    output_line = output_line + "\n      Real table: " + str(real_table)

                result_list.append(output_line)
                counter += 1

            elif args.format == "dict" or args.format == "json" or args.format == "jsonpretty":
                result_dict = dict([  ('oid', r['oid'])
                                    , ('schemaname', r['schemaname'])
                                    , ('objectname', r['objectname'])
                                    , ('objecttype', r['objecttype'])
                                    , ('size_bytes', int(r['size_bytes']))
                                    , ('live_tuple_count', int(r['live_tuple_count']))
                                    , ('live_tuple_percent', "{:.2f}".format(r['live_tuple_percent'])+"%" )
                                    , ('dead_tuple_count', int(r['dead_tuple_count']))
                                    , ('dead_tuple_size_bytes', int(r['dead_tuple_size_bytes']))
                                    , ('dead_tuple_percent', "{:.2f}".format(r['dead_tuple_percent'])+"%" )
                                    , ('free_space_bytes', int(r['free_space_bytes']))
                                    , ('free_percent', "{:.2f}".format(r['free_percent'])+"%" )
                                    , ('approximate', r['approximate'])
                                   ])
                result_list.append(result_dict)

        if args.format == "json":
            result_list = json.dumps(result_list)
        elif args.format == "jsonpretty":
            result_list = json.dumps(result_list, indent=4, separators=(',',': '))
    
        if len(result_list) >= 1:
            print_report(result_list)
        else:
            if args.quiet == 0:
                print("No bloat found for given parameters")

    close_conn(conn)
