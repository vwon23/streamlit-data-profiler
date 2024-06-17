import os, sys
import streamlit as st
import pandas as pd


## Find path of the script then find the app_run path
path_script = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(os.path.dirname(path_script)))

sys.path.append(path_app_run)

## use common functions to initalize global variable and define logger ##
import utilities.common_functions as cf
import queries.sf_queries as sfq
import queries.mssql_queries as msq


#### functions used for Streamlit ####
@st.cache_data
def st_initialize(path_app_run, loggername):
    st_logger = cf.initialize(path_app_run, loggername)
    return st_logger


## functions used to retreive data from Snowflake and return df ##
@st.cache_data
def list_sf_databases(sf_role, sf_wh):
    cf.gvar.sf_conn.execute_string(sfq.use_role_wh.format(sf_role=sf_role, sf_wh=sf_wh), return_cursors=False)
    df = cf.sf_exec_query_return_df(sfq.show_databases)
    list_dbs = df['name'].tolist()
    list_dbs.sort()
    return list_dbs

@st.cache_data
def list_sf_schemas(db):
    df = cf.sf_exec_query_return_df(sfq.list_schemas.format(db_name=db))
    list_schemas = df['TABLE_SCHEMA'].tolist()
    list_schemas.sort()
    return list_schemas

@st.cache_data
def list_sf_tables(db, schema):
    df = cf.sf_exec_query_return_df(sfq.list_tables.format(db_name=db, schema=schema))
    list_tables = df['TABLE_NAME'].tolist()
    list_tables.sort()
    return list_tables

@st.cache_data
def list_sf_columns(db, schema, table):
    df = cf.sf_exec_query_return_df(sfq.get_columns.format(db_name=db, schema=schema, table=table))
    df.sort_values(by='ORDINAL_POSITION', inplace=True)
    list_columns = df['COLUMN_NAME'].tolist()
    return list_columns

@st.cache_data
def return_sf_query_df(sql):
    df = cf.sf_exec_query_return_df(sql)
    return df



## functions used to retreive data from SQL Server and return df ##
@st.cache_data
def list_ms_schemas(_engine, db):
    df = cf.sal_exec_query_return_df(_engine, msq.list_schemas.format(db_name=db))
    list_schemas = df['table_schema'].tolist()
    list_schemas.sort()
    return list_schemas

@st.cache_data
def list_ms_tables(_engine, db, schema):
    df = cf.sal_exec_query_return_df(_engine, sfq.list_tables.format(db_name=db, schema=schema))
    list_tables = df['table_name'].tolist()
    list_tables.sort()
    return list_tables

@st.cache_data
def list_ms_columns(_engine, db, schema, table):
    df = cf.sal_exec_query_return_df(_engine, sfq.get_columns.format(db_name=db, schema=schema, table=table))
    df.sort_values(by='ordinal_position', inplace=True)
    list_columns = df['column_name'].tolist()
    return list_columns

@st.cache_data
def return_ms_query_df(_engine, sql):
    df = cf.sal_exec_query_return_df(_engine, sql)
    return df