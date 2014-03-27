#!/usr/bin/env python

import argparse, psycopg2, subprocess, sys
from psycopg2 import extras

# Bloat queries are adapted from the check_bloat query found in bucardo's check_postgres tool http://bucardo.org/wiki/Check_postgres

# TODO Set default subject to
#   --subject          [%Y-%m-%d %H:%M:%S %z] Bloat report for __mode__ in __dbname__ at __host__:__port__

parser = argparse.ArgumentParser(description="Provide a bloat report for PostgreSQL tables and/or indexes.")
args_general = parser.add_argument_group(title="General options")
args_general.add_argument('--mode', '-m', choices=["tables", "indexes"], default="tables", help="""Provide bloat report for the following objects: tables, indexes. Note that the "tables" mode does not include any index bloat that may also exist in the table. Default is "tables".""")
args_general.add_argument('-c','--connection', default="host=localhost", help="""Connection string for use by psycopg. Defaults to "host=localhost".""")
#args_general.add_argument('-f', '--format', default="table", choices=["table", "simple", "dict"], help="Output formats. Table outputs a table format sutable for a console. Simple is a plaintext version suitable for any output (preferred setting for email). Dict is a python dictionary object, which may be useful if taking input into another python script or for something that reads JSON.")
args_general.add_argument('-a', '--min_pages', type=int, default=1, help="Minimum number of pages an object must have to be included in the report. Default and minimum value is 1.")
args_general.add_argument('-A', '--min_wasted_pages', type=int, default=1, help="Minimum number of wasted pages an object must have to be included in the report. Default and minimum value is 1.")
args_general.add_argument('-p', '--min_wasted_percentage', type=float, default=0.1, help="Minimum percentage of wasted space an object must have to be included in the report. Default value is 0.1%%")
args_general.add_argument('-n', '--schema', help="Comma separated list of schema to include in report. All other schemas will be ignored.")
args_general.add_argument('-N', '--exclude_schema', help="Comma separated list of schemas to exclude. If set with -n, schemas will be excluded then included.")
args_general.add_argument('--view_schema', help="Set the schema that the bloat report view is in if it's not in the default search_path. Note this option can also be set when running --create_view to set in which schema you want the view created.")

args_mail = parser.add_argument_group(title="Email Report options")
args_mail.add_argument('-x', '--mailx', default="mailx", help="Full path to mailx binary if not in default path.")
args_mail.add_argument('-r', '--recipients', help="Comma separated list of recipients to send email report to.")
args_mail.add_argument('-s', '--subject', help="Subject for the email report.")
args_mail.add_argument('-z', '--send_zero', action="store_true", help="Send email even if nothing to report.")

args_setup = parser.add_argument_group(title="Setup")
args_setup.add_argument('--create_view', action="store_true", help="Create the required view that the bloat report uses. Places view in default search_path schema unless --view_schema is set.")
args_setup.add_argument('--create_mat_view', action="store_true", help="Same as --create_view, but creates it as materialized view if your version of PostgreSQL supports it (9.3+). Be aware that this script does not refresh the materialized view automatically.")
args = parser.parse_args()


def create_conn():
    conn = psycopg2.connect(args.connection)
    return conn


def close_conn(conn):
    conn.close()


def create_exclude_schema_list():
    if args.exclude_schema != None:
        split_list = args.exclude_schema.split(",")
    else:
        split_list = []
    split_list.append('pg_toast')
    return split_list


def create_include_schema_list():
    split_list = args.schema.split(",")
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
    cur.close()


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
            ELSE relpages::bigint - otta 
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


def get_index_bloat(connf):
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
            ELSE ipages::bigint - iotta 
          END AS wastedpages
        , CASE 
            WHEN ipages < iotta THEN 0 
            ELSE bs*(ipages-iotta) 
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
    ORDER BY wastedbytes DESC, schemaname ASC, iname ASC"""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    result = cur.fetchall()
    return result


if __name__ == "__main__":
    conn = create_conn()

    if args.create_view or args.create_mat_view:
        create_view(conn)
        close_conn(conn)
        sys.exit(1)

    if args.mode == "tables":
        result = get_table_bloat(conn)
    if args.mode == "indexes":
        result = get_index_bloat(conn)

    if args.schema != None:
        include_schema_list = create_include_schema_list()

    exclude_schema_list = create_exclude_schema_list()

    counter = 1
    for r in result:
        # Min check goes in order page, wasted_page, wasted_percentage to exclude things properly when options are combined
        if args.min_pages > 1:
            if r['pages'] <= args.min_pages:
                continue
        if args.min_wasted_pages > 1:
            if r['wastedpages'] <= args.min_wasted_pages:
                continue
        if args.min_wasted_percentage > 0.1:
            if r['bloat_percent'] <= args.min_wasted_percentage:
                continue

        if r['schemaname'] in exclude_schema_list:
            continue

        if args.schema != None:
            if r['schemaname'] not in include_schema_list:
                continue

#        if args.format == "simple":
        justify_space = 100 - len(str(counter)+". "+r['schemaname']+"."+r['objectname']+"(%)"+str(r['bloat_percent'])+r['wastedsize']+" wasted")
        print(str(counter) + ". " + r['schemaname'] + "." + r['objectname'] + "."*justify_space + "(" + str(r['bloat_percent']) + "%) " + r['wastedsize'] + " wasted")
        counter += 1

    close_conn(conn)
