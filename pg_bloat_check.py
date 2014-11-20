#!/usr/bin/env python

import argparse, psycopg2, sys
from psycopg2 import extras

version = "1.0.2"

# Bloat queries are adapted from the check_bloat query found in bucardo's check_postgres tool http://bucardo.org/wiki/Check_postgres

parser = argparse.ArgumentParser(description="Provide a bloat report for PostgreSQL tables and/or indexes. Note that the query to check for bloat can be extremely expensive on very large databases or those with many tables. The materialized view option in 9.3+ can help to mitigate getting bloat data back by storing it persistently instead of running a query every time the script is run (see --create_mat_view).")
args_general = parser.add_argument_group(title="General options")
args_general.add_argument('-m', '--mode', choices=["tables", "indexes"], default="tables", help="""Provide bloat report for the following objects: tables, indexes. Note that the "tables" mode does not include any index bloat that may also exist in the table. Default is "tables".""")
args_general.add_argument('-c','--connection', default="host=", help="""Connection string for use by psycopg. Defaults to "host=" (local socket).""")
# TODO Add an object filter option (external, return deliminated file)
# TODO see about a possible table format
args_general.add_argument('-f', '--format', default="simple", choices=["simple", "dict"], help="Output formats. Simple is a plaintext version suitable for any output (ex: console, pipe to email). Dict is a python dictionary object, which may be useful if taking input into another python script or something that needs a more structured format. Dict also provides more details about object pages. Default is simple.")
args_general.add_argument('-a', '--min_pages', type=int, default=1, help="Minimum number of pages an object must have to be included in the report. Default and minimum value is 1.")
args_general.add_argument('-A', '--min_wasted_pages', type=int, default=1, help="Minimum number of wasted pages an object must have to be included in the report. Default and minimum value is 1.")
args_general.add_argument('-z', '--min_wasted_size', type=int, default=1, help="Minimum size of wasted space in bytes. Default and minimum is 1.")
args_general.add_argument('-p', '--min_wasted_percentage', type=float, default=0.1, help="Minimum percentage of wasted space an object must have to be included in the report. Default and minimum value is 0.1%%.")
args_general.add_argument('-n', '--schema', help="Comma separated list of schema to include in report. All other schemas will be ignored.")
args_general.add_argument('-N', '--exclude_schema', help="Comma separated list of schemas to exclude. If set along with -n, schemas will be excluded then included.")
args_general.add_argument('-e', '--exclude_object_file', help="""Full path to file containing a return deliminated list of objects to exclude from the report (tables and/or indexes). All objects must be schema qualified. Comments are allowed if the line is prepended with "#".""")
args_general.add_argument('--version', action="store_true", help="Print version of this script.")

args_setup = parser.add_argument_group(title="Setup")
args_general.add_argument('--view_schema', help="Set the schema that the bloat report view is in if it's not in the default search_path. Note this option can also be set when running --create_view to set in which schema you want the view created.")
args_setup.add_argument('--create_view', action="store_true", help="Create the required view that the bloat report uses. Places view in default search_path schema unless --view_schema is set.")
args_setup.add_argument('--create_mat_view', action="store_true", help="Same as --create_view, but creates it as materialized view if your version of PostgreSQL supports it (9.3+). Be aware that this script does not refresh the materialized view automatically.")
args = parser.parse_args()


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
        try:
            fh = open(list_items, 'r')
            for line in fh:
                if not line.strip().startswith('#'):
                    split_list.append(line.strip())
        except IOError as e:
           print("Cannot access exclude file " + list_items + ": " + e.strerror)
           sys.exit(2)

    return split_list


def create_view(conn):
    sql = "CREATE"
    if args.create_mat_view:
        sql += " MATERIALIZED"
    sql += " VIEW "
    if args.view_schema != None:
        sql += args.view_schema + "."
    sql += "bloat_view AS "
    sql += """ 
    SELECT
        nn.nspname AS schemaname
        , cc.relname AS tablename
        , COALESCE(cc.reltuples,0) AS reltuples
        , COALESCE(cc.relpages,0) AS relpages
        , COALESCE(bs,0) AS bs
        , COALESCE(CEIL((cc.reltuples*((datahdr+ma- 
                            (CASE WHEN datahdr%ma = 0 THEN ma ELSE datahdr%ma END)
                        ) + nullhdr2 + 4)) / (bs - 20::float)),0) AS otta
        , COALESCE(c2.relname,'?') AS iname
        , COALESCE(c2.reltuples,0) AS ituples
        , COALESCE(c2.relpages,0) AS ipages
        , COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
    FROM
    pg_class cc
    JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname <> 'information_schema'
    LEFT JOIN
    (
        SELECT
            ma
            , bs
            , foo.nspname
            , foo.relname
            , (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr
            , (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
        FROM (
            SELECT
                ns.nspname
                , tbl.relname
                , hdr
                , ma
                , bs
                , SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth
                , MAX(coalesce(null_frac,0)) AS maxfracsum
                , hdr + (
                    SELECT 1+count(*)/8
                    FROM pg_stats s2
                    WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
                    ) AS nullhdr
            FROM pg_attribute att
            JOIN pg_class tbl ON att.attrelid = tbl.oid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
                AND s.tablename = tbl.relname
                AND s.inherited=false
                AND s.attname=att.attname
            , (
                SELECT
                    ( SELECT current_setting('block_size')::numeric) AS bs
                    , CASE WHEN SUBSTRING(SPLIT_PART(v, ' ', 2) FROM '#"[0-9]+.[0-9]+#"%' for '#')
                        IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr
                    , CASE WHEN v ~ 'mingw32' OR v ~ '64-bit' THEN 8 ELSE 4 END AS ma
                FROM (SELECT version() AS v) AS foo
            ) AS constants
            WHERE att.attnum > 0 AND tbl.relkind='r'
            GROUP BY 1,2,3,4,5
        ) AS foo
    ) AS rs
    ON cc.relname = rs.relname AND nn.nspname = rs.nspname
    LEFT JOIN pg_index i ON indrelid = cc.oid
    LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    sql = "COMMENT ON VIEW bloat_view IS 'View providing raw data for current table & index bloat status'"
    cur.execute(sql)
    conn.commit()
    cur.close()


def get_index_bloat(conn):
    sql = """
    SELECT
        current_database() AS db
        , schemaname
        , iname as objectname
        , ipages::bigint AS pages
        , ROUND(CASE 
                    WHEN iotta=0 AND ipages>0 THEN 100.0
                    WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 
                    ELSE ((ipages-iotta)::numeric/ipages)*100
                END
            , 1) AS bloat_percent
        , CASE 
            WHEN ipages < iotta THEN 0 
            ELSE (ipages - iotta)::bigint
          END AS wastedpages
        , CASE 
            WHEN ipages < iotta THEN 0 
            ELSE bs*(ipages-iotta)::bigint
          END AS wastedbytes
        , CASE 
            WHEN ipages < iotta THEN '0 bytes' 
            ELSE pg_size_pretty((bs*(ipages-iotta))::bigint)
          END AS wastedsize
    FROM """
    if args.view_schema != None:
        sql += args.view_schema + "."
    sql += """bloat_view
    WHERE iname <> '?' 
    AND ipages > 1
    AND (ipages-iotta) > 1
    ORDER BY wastedbytes DESC, schemaname ASC, tablename ASC"""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    result = cur.fetchall()
    return result


def get_table_bloat(conn):
    sql = """
    SELECT
        distinct current_database() AS db
        , schemaname
        , tablename AS objectname
        , relpages::bigint AS pages
        , ROUND(CASE 
                    WHEN otta=0 AND relpages>0 THEN 100.0
                    WHEN otta=0 OR relpages=0 OR relpages=otta THEN 0.0 
                    ELSE ((relpages-otta)::numeric/relpages)*100
            END,1) AS bloat_percent 
        , CASE 
            WHEN relpages < otta THEN 0 
            ELSE (relpages - otta)::bigint
          END AS wastedpages
        , CASE 
            WHEN relpages < otta THEN 0 
            ELSE bs*(relpages-otta)::bigint 
          END AS wastedbytes
        , CASE 
            WHEN relpages < otta THEN '0 bytes'::text 
            ELSE pg_size_pretty((bs*(relpages-otta))::bigint)
          END AS wastedsize
      FROM """
    if args.view_schema != None:
        sql += args.view_schema + "."
    sql += """bloat_view
    WHERE iname <> '?'
    AND relpages > 1
    AND (relpages-otta) > 0
    ORDER BY wastedbytes DESC, schemaname ASC, tablename ASC"""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    result = cur.fetchall()
    return result


def print_report(result_list):
    for r in result_list:
        print(r)


def print_version():
    print("Version: " + version)


if __name__ == "__main__":
    if args.version:
        print_version()
        sys.exit(1)

    conn = create_conn()

    if args.create_view or args.create_mat_view:
        create_view(conn)
        close_conn(conn)
        sys.exit(1)

    if args.mode == "tables":
        result = get_table_bloat(conn)
    if args.mode == "indexes":
        result = get_index_bloat(conn)

    close_conn(conn)

    if args.schema != None:
        include_schema_list = create_list('csv', args.schema)
    else:
        include_schema_list = []

    if args.exclude_schema != None:
        exclude_schema_list = create_list('csv', args.exclude_schema)
    else:
        exclude_schema_list = []
    exclude_schema_list.append('pg_toast')

    if args.exclude_object_file != None:
        exclude_object_list = create_list('file', args.exclude_object_file)
    else:
        exclude_object_list = []

    counter = 1
    result_list = []
    for r in result:
        # Min check goes in order page, wasted_page, wasted_size, wasted_percentage to exclude things properly when options are combined
        if args.min_pages > 1:
            if r['pages'] < args.min_pages:
                continue
        elif args.min_pages < 1:
            print("--min_pages (-a) must be >= 1")
            sys.exit(2)

        if args.min_wasted_pages > 1:
            if r['wastedpages'] < args.min_wasted_pages:
                continue
        elif args.min_wasted_pages < 1:
            print("--min_wasted_pages (-A) must be >= 1")
            sys.exit(2)

        if args.min_wasted_size > 1:
            if r['wastedbytes'] < args.min_wasted_size:
                continue
        elif args.min_wasted_size < 1:
            print("--min_wasted_size (-z) must be >= 1")
            sys.exit(2)

        if float(args.min_wasted_percentage) > float(0.1):
            if float(r['bloat_percent']) < float(args.min_wasted_percentage):
                continue
        elif float(args.min_wasted_percentage) < float(0.1):
            print("--min_wasted_percentage (-p) must be >= 0.1%%")
            sys.exit(2)

        if r['schemaname'] in exclude_schema_list:
            continue

        if ( len(include_schema_list) > 0 and r['schemaname'] not in include_schema_list ):
            continue

        if ( len(exclude_object_list) > 0 and
                (r['schemaname'] + "." + r['objectname']) in exclude_object_list ):
            continue

        if args.format == "simple":
            justify_space = 100 - len(str(counter)+". "+r['schemaname']+"."+r['objectname']+"(%)"+str(r['bloat_percent'])+r['wastedsize']+" wasted")
            result_list.append(str(counter) + ". " + r['schemaname'] + "." + r['objectname'] + "."*justify_space + "(" + str(r['bloat_percent']) + "%) " + r['wastedsize'] + " wasted")
            counter += 1
        elif args.format == "dict":
            result_dict = dict([('schemaname', r['schemaname'])
                                , ('objectname', r['objectname'])
                                , ('total_pages', int(r['pages']) )
                                , ('bloat_percent', str(r['bloat_percent'])+"%" )
                                , ('wasted_size', r['wastedsize'])
                                , ('wasted_pages', int(r['wastedpages']))
                                ])
            result_list.append(result_dict)

    if len(result_list) >= 1:
        print_report(result_list)
    else:
        print("No bloat found for given parameters")

"""
LICENSE AND COPYRIGHT
---------------------

pg_bloat_check.py is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2014 Keith Fiske

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
"""
