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



#### Streamlit code starts here ####
st.set_page_config(page_title="Snowflake Data Profile", page_icon="📊")
logger_name = 'snowflake_data_profile'
logger = cf.st_initialize(path_app_run, logger_name)

st.markdown("# Snowflake Data PandasProfile")
st.write("This page uses snowflake-connector to query snowflake data and runs pandas profiling on queried data.")
st.divider()


## functions to connect to snowflake and list databases, schemas, tables ##
if 'connect_to_sf' not in st.session_state:
    st.session_state.connect_to_sf = False

if 'connect_to_sf_clicked' not in st.session_state:
    st.session_state.connect_to_sf_clicked = False


def click_connect_sf():
    st.session_state.connect_to_sf = True
    st.session_state.connect_to_sf_clicked = True

## Connect to Snowflake then set the session state to False to prevent reconnection ##
@st.cache_data
def connect_to_sf(sf_user, sf_role, sf_wh):
    cf.connect_snowflake_login(sf_user, sf_role, sf_wh)
    #cf.connect_snowflake_sso(sf_user, sf_role, sf_wh)
    st.session_state.connect_to_sf = False

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



## Create a form to enter Snowflake login details ##
with st.sidebar.form('sf_connection_form'):
    st.header("Snowflake Login")
    # sf_account = st.sidebar.text_input("Account:", cf.gvar.sf_account)
    sf_user_input = st.text_input("User:", cf.gvar.sf_username)
    sf_role_input = st.text_input("Role:", cf.gvar.sf_app_role)
    sf_wh_input = st.text_input("Warehouse:", cf.gvar.sf_app_wh)

    submitted = st.form_submit_button('Connect to Snowflake')
    if submitted:
        click_connect_sf()

# st.sidebar.header("Snowflake Login")
# # sf_account = st.sidebar.text_input("Account:", cf.gvar.sf_account)
# sf_user_input = st.sidebar.text_input("User:", cf.gvar.sf_username)
# sf_role_input = st.sidebar.text_input("Role:", cf.gvar.sf_admin_role)
# sf_wh_input = st.sidebar.text_input("Warehouse:", cf.gvar.sf_admin_wh)
# st.sidebar.button('Connect to Snowflake', on_click=click_connect_sf)


## When connect to snowflake button is clicked, connect to snowflake ##
if st.session_state.connect_to_sf:
    st.session_state.sf_user = sf_user_input
    st.session_state.sf_role = sf_role_input
    st.session_state.sf_wh = sf_wh_input
    try:
        connect_to_sf(st.session_state.sf_user, st.session_state.sf_role, st.session_state.sf_wh)
    except Exception as e:
        st.error(f'Error connecting to Snowflake: {e}')

## If connected to snowflake, show databases, schemas, tables and generated SQL to query ##
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
    st.session_state.sql_text_area = updated_sql

def update_rows_to_limit(sql):
    st.session_state.rows_to_limit = st.session_state.limit_rows
    update_sql(sql)

def update_where_clause(sql):
    st.session_state.updated_where_clause = st.session_state.where_clause
    update_sql(sql)

def update_order_clause(sql):
    st.session_state.updated_order_by = st.session_state.order_by_clause
    update_sql(sql)

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
            st.session_state.rows_to_limit = 500000

        if 'updated_where_clause' not in st.session_state:
            st.session_state.updated_where_clause = ''

        if 'updated_order_by' not in st.session_state:
            st.session_state.updated_order_by = ''

        if 'sql_query' not in st.session_state:
            st.session_state.sql_query = st.session_state.sql_base + f'\nlimit {st.session_state.rows_to_limit}'


        limit_rows, where_clause, order_by_clause = st.columns(3)
        limit_rows.number_input("limit rows:", value=st.session_state.rows_to_limit, step=100000, key='limit_rows', on_change=update_rows_to_limit, args=[st.session_state.sql_base])
        where_clause.text_input("where:", value=st.session_state.updated_where_clause, key='where_clause',  on_change=update_where_clause, args=[st.session_state.sql_base])
        order_by_clause.text_input("order by:", value=st.session_state.updated_order_by, key='order_by_clause',  on_change=update_order_clause, args=[st.session_state.sql_base])

        update_sql(st.session_state.sql_base)


        sql_text_area = st.text_area(
            label="SQL to send to Snowflake",
            key="sql_text_area",
            )

        st.button('Execute SQL to query data')

    except Exception as e:
        st.error(f'Error: {e}')