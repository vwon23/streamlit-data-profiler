import os, sys
import streamlit as st
import pandas as pd

from ydata_profiling import ProfileReport
from streamlit_ydata_profiling import st_profile_report

import dtale


## Find path of the script then find the app_run path
path_script = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(os.path.dirname(path_script)))

sys.path.append(path_app_run)

## use common functions to initalize global variable and define logger ##
import utilities.common_functions as cf
import queries.sf_queries as sfq


#### Streamlit code starts here ####
st.set_page_config(page_title="Snowflake Data Profile",layout="wide", page_icon="📊")
logger_name = 'snowflake_data_profile'
logger = cf.st_initialize(path_app_run, logger_name)

st.markdown("# Snowflake Data PandasProfile")
st.write("This page uses snowflake-connector to query snowflake data and runs pandas profiling on queried data.")
st.divider()


## session states used to toggle displays ##
if 'sf_sso_login' not in st.session_state:
    st.session_state.sf_sso_login = True

if 'connect_to_sf' not in st.session_state:
    st.session_state.connect_to_sf = False

if 'connect_to_sf_clicked' not in st.session_state:
    st.session_state.connect_to_sf_clicked = False

if 'query_submitted' not in st.session_state:
    st.session_state.query_submitted = False

if 'display_df' not in st.session_state:
    st.session_state.display_df = False

if 'display_pandas_profile' not in st.session_state:
    st.session_state.display_pandas_profile = False

if 'display_dtale_profile' not in st.session_state:
    st.session_state.display_dtale_profile = False

## Session state to toggle connection to Snowflake ##
def click_connect_sf():
    st.session_state.sf_sso_login = st.session_state.sf_sso_checkbox
    st.session_state.connect_to_sf = True
    st.session_state.connect_to_sf_clicked = True

## Connect to Snowflake then set the session state to False to prevent reconnection ##
@st.cache_data
def connect_to_sf(sf_user, sf_role, sf_wh):
    if st.session_state.sf_sso_login:
        cf.connect_snowflake_sso(sf_user, sf_role, sf_wh)
    else:
        cf.connect_snowflake_login(sf_user, sf_role, sf_wh)
    st.session_state.connect_to_sf = False



## Create a form to enter login information to connect to Snowflake ##
with st.sidebar.form('sf_connection_form'):
    st.header("Snowflake Login")

    sf_user_input = st.text_input("User:", cf.gvar.sf_username)
    sf_role_input = st.text_input("Role:", cf.gvar.sf_app_role)
    sf_wh_input = st.text_input("Warehouse:", cf.gvar.sf_app_wh)

    sf_sso_checkbox = st.checkbox("Use SSO", key='sf_sso_checkbox', value=st.session_state.sf_sso_login)
    connect_button = st.form_submit_button('Connect to Snowflake')
    if connect_button:
        click_connect_sf()


## When connect to snowflake button is clicked, connect to snowflake ##
if st.session_state.connect_to_sf:
    st.session_state.sf_user = sf_user_input
    st.session_state.sf_role = sf_role_input
    st.session_state.sf_wh = sf_wh_input
    try:
        connect_to_sf(st.session_state.sf_user, st.session_state.sf_role, st.session_state.sf_wh)
    except Exception as e:
        st.error(f'Error connecting to Snowflake: {e}')



## If connected to snowflake, show databases, schemas, tables and generated SQL to query.##
def generate_sql_base(db, schema, table):
    sql = f'select *\nfrom {db}.{schema}.{table}'
    st.session_state.sql_base = sql

def update_sql(sql):
    if st.session_state.updated_where_clause.strip() != '':
        where_clause = f'\nwhere {st.session_state.updated_where_clause}'
    else:
        where_clause = ''

    if st.session_state.updated_order_by.strip() != '':
        order_by_clause = f'\norder by {st.session_state.updated_order_by}'
    else:
        order_by_clause = ''

    if st.session_state.rows_to_limit:
        limit_rows_clause = f'\nlimit {str(st.session_state.rows_to_limit)}'
    else:
        limit_rows_clause = ''

    updated_sql = sql + where_clause + order_by_clause + limit_rows_clause
    st.session_state.sql_query = updated_sql
    st.session_state.sql_text_area_updated = updated_sql


## Use st.session_state to keep values when widgets are updated ##
def update_rows_to_limit(sql):
    st.session_state.rows_to_limit = st.session_state.limit_rows
    update_sql(sql)

def update_where_clause(sql):
    st.session_state.updated_where_clause = st.session_state.where_clause
    update_sql(sql)

def update_order_clause(sql):
    st.session_state.updated_order_by = st.session_state.order_by_clause
    update_sql(sql)

def submit_query():
    st.session_state.query_submitted = True

## Session state to toggle connection to Snowflake ##
def click_connect_sf():
    st.session_state.connect_to_sf = True
    st.session_state.connect_to_sf_clicked = True

def profile_data_panda():
    st.session_state.display_pandas_profile = True
    st.session_state.run_pandas_profile = True

def profile_data_dtale(df):
    st.session_state.display_dtale_profile = True
    st.session_state.dtale = dtale.show(df, host='localhost')
    st.session_state.dtale.open_browser()

def stop_dtale():
    st.session_state.dtale.kill()
    st.session_state.display_dtale_profile = False


## functions used to retreive data from snowflake and return df ##
@st.cache_data
def list_databases(sf_role, sf_wh):
    cf.gvar.sf_conn.execute_string(sfq.use_role_wh.format(sf_role=sf_role, sf_wh=sf_wh), return_cursors=False)
    df = cf.sf_exec_query_return_df(sfq.show_databases)
    list_dbs = df['name'].tolist()
    return list_dbs

@st.cache_data
def list_schemas(db):
    list_schemas = cf.sf_exec_query_return_df(sfq.list_schemas.format(db_name=db))
    return list_schemas

@st.cache_data
def list_tables(db, schema):
    list_tables = cf.sf_exec_query_return_df(sfq.list_tables.format(db_name=db, schema=schema))
    return list_tables

@st.cache_data
def return_query_df(sql):
    df = cf.sf_exec_query_return_df(sql)
    return df


if st.session_state.connect_to_sf_clicked:
    try:
        column_db, column_schema, column_table = st.columns(3)

        list_dbs = list_databases(st.session_state.sf_role, st.session_state.sf_wh)
        db_selected = column_db.selectbox(
        "Select a database:",
        list_dbs)

        df_schemas = list_schemas(db_selected)
        schema_selected = column_schema.selectbox(
        "Select a schema:",
        df_schemas)

        df_tables = list_tables(db_selected, schema_selected)
        table_selected = column_table.selectbox(
        "Select a table:",
        df_tables)

        st.write("You selected:", db_selected + '.' + schema_selected + '.' + table_selected)
        generate_sql_base(db_selected, schema_selected, table_selected)

        if 'rows_to_limit' not in st.session_state:
            st.session_state.rows_to_limit = 100000

        if 'updated_where_clause' not in st.session_state:
            st.session_state.updated_where_clause = ''

        if 'updated_order_by' not in st.session_state:
            st.session_state.updated_order_by = ''

        if 'sql_query' not in st.session_state:
            st.session_state.sql_query = st.session_state.sql_base + f'\nlimit {st.session_state.rows_to_limit}'


        limit_rows, where_clause, order_by_clause = st.columns(3)
        limit_rows.number_input("limit rows:", value=st.session_state.rows_to_limit, min_value=1, step=50000, key='limit_rows', on_change=update_rows_to_limit, args=[st.session_state.sql_base])
        where_clause.text_input("where:", value=st.session_state.updated_where_clause, key='where_clause',  on_change=update_where_clause, args=[st.session_state.sql_base])
        order_by_clause.text_input("order by:", value=st.session_state.updated_order_by, key='order_by_clause',  on_change=update_order_clause, args=[st.session_state.sql_base])

        update_sql(st.session_state.sql_base)

        sql_text_area = st.text_area(
            label="SQL to send to Snowflake (Ctrl+Enter to update query manually):",
            key="sql_text_area",
            value=st.session_state.sql_text_area_updated,
            height=145
            )

        ## Preview of query to submit and submit button ##
        sq_preview = st.write('Preview of query to submit: ' + sql_text_area[0:40] + ' ...... ' + sql_text_area[-40:])
        submit_query_button = st.button('Submit SQL to query data',
                  on_click=submit_query)

    except Exception as e:
        st.error(f'Error: {e}')


## When Execute SQL button  button is clicked, connect to snowflake and return result into dataframe ##
if st.session_state.query_submitted:
    try:
        df = return_query_df(sql_text_area)
        st.session_state.df = df
        st.session_state.display_df = True
        st.session_state.display_entire_df = False
        st.session_state.display_pandas_profile = False
        st.session_state.query_submitted = False
    except Exception as e:
        st.error(f'Error: {e}')

if st.session_state.display_df:
    if st.session_state.display_entire_df:
        st.write("All of queried data:")
        st.write(st.session_state.df)
    else:
        st.write("Displaying first 100 rows of queried data:")
        st.session_state.df[:100]

    st.checkbox('Display all of queried data', key='display_entire_df', value=st.session_state.display_entire_df)
    st.button('Profile Data using Pandas Profile', on_click=profile_data_panda)
    st.button('Profile Data using Dtale', on_click=profile_data_dtale, args=[st.session_state.df])


## Profile the dataframe and provide profile report##
if st.session_state.display_pandas_profile:
    if st.session_state.run_pandas_profile:
        try:
            pr = ProfileReport(st.session_state.df,
                               title=db_selected + '.' + schema_selected + '.' + table_selected,
                               minimal=True,
                               correlations={"cramers": {"calculate": False}},
                               orange_mode=True)
            logger.info(f'Profile report created for {db_selected}.{schema_selected}.{table_selected}')
            st.session_state.run_pandas_profile = False
        except Exception as e:
            st.error(f'Error: {e}')
    st_profile_report(pr, navbar=True)

## Profile the dataframe using dtale library ##
if st.session_state.display_dtale_profile:
    st.button('Stop running dtale', key='stop_dtale_button', on_click=stop_dtale)
