import os, sys
import streamlit as st
import pandas as pd

from ydata_profiling import ProfileReport
from streamlit_ydata_profiling import st_profile_report
import dtale

import utils.st_functions as stf


## Find path of the script then find the app_run path
path_script = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(os.path.dirname(path_script)))

sys.path.append(path_app_run)

## use common functions to initalize global variable and define logger ##
import utilities.common_functions as cf


#### Streamlit code starts here ####
st.set_page_config(page_title="Snowflake Data Profile",layout="wide", page_icon="📊")
logger_name = 'snowflake_data_profile'
logger = stf.st_initialize(path_app_run, logger_name)

st.markdown("# Snowflake Data Profile")
st.write("This page uses snowflake-connector to connect to Snowflake query data. Then queried data can be profiled using pandas profiling or dtale.")
st.divider()


## session states used to toggle displays ##
if 'connect_to_sf' not in st.session_state:
    st.session_state.connect_to_sf = False

if 'connect_to_sf_clicked' not in st.session_state:
    st.session_state.connect_to_sf_clicked = False

if 'display_df' not in st.session_state:
    st.session_state.display_df = False

if 'display_entire_df' not in st.session_state:
    st.session_state.display_entire_df = False

if 'display_pandas_profile' not in st.session_state:
    st.session_state.display_pandas_profile = False

if 'pandas_profile_failed' not in st.session_state:
    st.session_state.pandas_profile_failed = False

if 'df_converted' not in st.session_state:
    st.session_state.df_converted = False

if 'df_reverted' not in st.session_state:
    st.session_state.df_reverted = False

if 'dtale_running' not in st.session_state:
    st.session_state.dtale_running = False


## Form to enter login information to connect to Snowflake ##
def click_connect_sf():
    st.session_state.connect_to_sf = True
    st.session_state.connect_to_sf_clicked = True

with st.sidebar.form('sf_connection_form'):
    st.header("Snowflake Login")

    sf_user_input = st.text_input("User:", cf.gvar.sf_username)
    sf_role_input = st.text_input("Role:", cf.gvar.sf_app_role)
    sf_wh_input = st.text_input("Warehouse:", cf.gvar.sf_app_wh)

    sf_sso_checkbox = st.checkbox("Use SSO", key='sf_sso_checkbox', value=True)
    connect_button = st.form_submit_button('Connect to Snowflake')

    if connect_button:
        click_connect_sf()


### 1. Connect to Snowflake when "Connect to Snowflake" button is clicked. ###
## Connect to snowflake and set the session state to False to prevent reconnection ##
@st.cache_data
def connect_to_sf(sf_user, sf_role, sf_wh, sso):
    if sso:
        cf.connect_snowflake_sso(sf_user, sf_role, sf_wh)
    else:
        cf.connect_snowflake_login(sf_user, sf_role, sf_wh)
    st.session_state.connect_to_sf = False

if st.session_state.connect_to_sf:
    st.session_state.sf_user = sf_user_input
    st.session_state.sf_role = sf_role_input
    st.session_state.sf_wh = sf_wh_input
    try:
        connect_to_sf(st.session_state.sf_user, st.session_state.sf_role, st.session_state.sf_wh, st.session_state.sf_sso_checkbox)
    except Exception as e:
        st.error(f'Error connecting to Snowflake: {e}')



### 2. If connected to snowflake, show databases, schemas, tables and generated SQL to query. ###
## functions for generating SQL to query data ##
def generate_sql_base(db, schema, table):
    list_columns = stf.list_sf_columns(db, schema, table)
    str_columns = ', '.join(list_columns)
    sql = f'select  {str_columns}\nfrom  {db}.{schema}.{table}'
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


## Use st.session_state to keep string values when widgets are updated ##
def update_rows_to_limit(sql):
    st.session_state.rows_to_limit = st.session_state.limit_rows
    update_sql(sql)

def update_where_clause(sql):
    st.session_state.updated_where_clause = st.session_state.where_clause
    update_sql(sql)

def update_order_clause(sql):
    st.session_state.updated_order_by = st.session_state.order_by_clause
    update_sql(sql)


### 3. When Execute SQL button  button is clicked, connect to snowflake and return result into dataframe display the df ###
## functions to reset display states and submit query ##
def reset_df_display():
    st.session_state.display_df = False
    st.session_state.display_entire_df = False
    st.session_state.display_pandas_profile = False
    st.session_state.pandas_profile_failed = False
    st.session_state.df_converted = False
    st.session_state.pandas_profile_complete = False
    st.session_state.dtale_running = False

def submit_query():
    reset_df_display()
    try:
        df = stf.return_sf_query_df(sql_text_area)
        st.session_state.sql_text_submitted = sql_text_area
        st.session_state.df = df
        st.session_state.display_df = True
    except Exception as e:
        st.error(f'Error: {e}')
        logger.error(f'Error: {e}')


if st.session_state.connect_to_sf_clicked:
    try:
        column_db, column_schema, column_table = st.columns(3)

        list_dbs = stf.list_sf_databases(st.session_state.sf_role, st.session_state.sf_wh)
        db_selected = column_db.selectbox(
        "Select a database:",
        list_dbs)

        df_schemas = stf.list_sf_schemas(db_selected)
        schema_selected = column_schema.selectbox(
        "Select a schema:",
        df_schemas)

        df_tables = stf.list_sf_tables(db_selected, schema_selected)
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
        sq_preview, submit_query_button = st.columns([4, 1])
        sq_preview.write('Preview of query to submit: ' + sql_text_area[0:50] + ' ...... ' + sql_text_area[-50:])
        submit_query_button.button('Submit SQL to query data', on_click=submit_query)


    except Exception as e:
        st.error(f'Error: {e}')
        st.session_state.connect_to_sf_clicked = False








### 4. Profile the dataframe using pandas profiling library or dtale library ###

## functions called to proflie dataframe when profile buttons are clicked ##
def profile_data_panda(df):
    st.session_state.display_pandas_profile = True
    st.session_state.pandas_profile_failed = False
    st.session_state.pandas_profile_complete = False

    try:
        st.session_state.pr = ProfileReport(df,
                                title=db_selected + '.' + schema_selected + '.' + table_selected,
                                minimal=True,
                                correlations={"cramers": {"calculate": False}},
                                orange_mode=True)
    except Exception as e:
        st.error(f'Error: {e}')
        logger.error(f'Error: {e}')


## save pandas profile report to output folder ##
def save_profile_report(pr):
    file_name = 'test_profile_report.html'
    output_file_path = os.path.join(cf.gvar.path_outputs, file_name)

    try:
        pr.to_file(output_file_path)
        logger.info(f'Profile report saved to {output_file_path}')
        st.success(f'Profile report saved to {output_file_path}')
        # os.system(f'start {cf.gvar.path_outputs}')
    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error: {e}')


def profile_data_dtale(df):
    if not st.session_state.dtale_running:
        st.session_state.display_pandas_profile = False
        st.session_state.pandas_profile_complete = False
        st.session_state.dtale = dtale.show(df, host='localhost')
        st.session_state.dtale.open_browser()
        st.session_state.dtale_running = True
    else:
        st.session_state.dtale.kill()
        st.session_state.dtale_running = False


## Convert object columns to string values to avoid pandas profiling errors ##
def convert_df_obj_to_str(df):
    for col in df.columns:
        if df[col].dtype.name == 'object':
            df[col] = df[col].astype(str)
    logger.info('Pandas Dataframe Object columns converted to string values')
    st.session_state.df_converted = True
    st.session_state.df_reverted = False
    st.session_state.pandas_profile_failed = False


def revert_df_original():
    df = stf.return_sf_query_df(st.session_state.sql_text_submitted)
    st.session_state.df = df
    st.session_state.pandas_profile_failed = False
    st.session_state.df_reverted = True


## display dataframe of queried data along with profile buttons ##
if st.session_state.display_df:
    if st.session_state.display_entire_df:
        st.write("All of queried data:")
        st.session_state.df
    else:
        st.write("Displaying first 1000 rows of queried data:")
        st.session_state.df[:1000]

    ## define streamlit objects and size of columns ##
    diplay_all_df, convert_df_button, gap1, pandas_profile_label, pandas_profile_button, output_profile_button, gap2, dtale_label, dtale_profile_button = st.columns([4, 3, 0.7, 2.3, 2, 2, 0.7, 2.3, 2])

    diplay_all_df.checkbox('Display all of queried data', key='display_entire_df', value=st.session_state.display_entire_df)


    pandas_profile_label.text('Pandas Profile:')

    dtale_label.text('Dtale Profile:')

    if not st.session_state.dtale_running:
        dtale_button_text = 'Run dtale to profile data'
    else:
        dtale_button_text = 'Stop running dtale'
    dtale_profile_button.button(dtale_button_text, on_click=profile_data_dtale, args=[st.session_state.df])


## Display Pandas Profile report ##
if st.session_state.display_pandas_profile:
    try:
        st_profile_report(st.session_state.pr, navbar=True)
        st.session_state.pandas_profile_complete = True
    except Exception as e:
        st.error(f'Error: {e}')
        logger.error(f'Error while creating pandas profilng report')
        st.session_state.display_pandas_profile = False
        st.session_state.pandas_profile_failed = True


## buttons to toggle based on pandas profiling result ##
if st.session_state.display_df:
    if not st.session_state.pandas_profile_failed and not st.session_state.pandas_profile_complete and not st.session_state.dtale_running:
        pandas_profile_button.button('Generate profile report', on_click=profile_data_panda, args=[st.session_state.df])

    if st.session_state.pandas_profile_failed and not st.session_state.df_converted:
        convert_df_button.button('Convert values to string (For Pandas Profile Error)', on_click=convert_df_obj_to_str, args=[st.session_state.df])
        pandas_profile_button.write('Profiling failed. Please convert object columns to string values to avoid errors.')

    if st.session_state.df_converted and not st.session_state.df_reverted:
        convert_df_button.button('Revert back to original DataFrame', on_click=revert_df_original)

    if st.session_state.pandas_profile_complete and not st.session_state.pandas_profile_failed:
        output_profile_button.button('Save profile report', on_click=save_profile_report, args=[st.session_state.pr])