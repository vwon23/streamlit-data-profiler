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
st.set_page_config(page_title="MSSQL Data Profile",layout="wide", page_icon="📊")
database_name = 'ms_sql_server'
logger_name = f'{database_name}_data_profile'
logger = stf.st_initialize(path_app_run, logger_name)

st.markdown("# Microsoft SQL Server Data Profile")
st.write("This page uses SQL Alchemy library to connect to SQL Server to query data. Then queried data can be profiled using pandas profiling or dtale.")
st.divider()


## session states used to toggle displays ##
if 'connected_to_ms' not in st.session_state:
    st.session_state.connected_to_ms = False

if 'display_df_ms' not in st.session_state:
    st.session_state.display_df_ms = False

if 'display_entire_df_ms' not in st.session_state:
    st.session_state.display_entire_df_ms = False

if 'display_pandas_profile_ms' not in st.session_state:
    st.session_state.display_pandas_profile_ms = False

if 'pandas_profile_failed_ms' not in st.session_state:
    st.session_state.pandas_profile_failed_ms = False

if 'df_converted_ms' not in st.session_state:
    st.session_state.df_converted_ms = False

if 'df_reverted_ms' not in st.session_state:
    st.session_state.df_reverted_ms = False

if 'dtale_running' not in st.session_state:
    st.session_state.dtale_running = False

if 'processing' not in st.session_state:
    st.session_state.processing = False


### 1. Connect to Snowflake when "Connect to Snowflake" button is clicked. ###
## Connect to snowflake and set the session state to False to prevent reconnection ##
# @st.cache_data
# @st.cache_resource(hash_funcs={sal.engine.base.Engine: id})
# @st.cache(allow_output_mutation=True)
@st.cache_resource
def connect_to_mssql(server, database, windows_auth):
    try:
        engine = cf.sal_create_enginem_ms_sql(server=server, database=database, windows_auth=windows_auth)

        st.session_state.server_ms = server
        st.session_state.database_ms = database
        st.session_state.engine_ms = engine
        st.session_state.connected_to_ms = True
    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error connecting to SQL Server: {e}')

## Form to enter login information to connect to Snowflake ##
with st.sidebar.form('mssql_connection_form'):
    st.header("SQL Server Login")

    server_input = st.text_input("Server:", cf.gvar.mssql_server)
    database_input = st.text_input("Database:", cf.gvar.mssql_database)

    windows_auth_checkbox = st.checkbox("Use Windows Auth", value=True)
    connect_button = st.form_submit_button('Connect to SQL Server')

    if connect_button:
        connect_to_mssql(server_input, database_input, windows_auth_checkbox)




### 2. If connected to snowflake, show databases, schemas, tables and generated SQL to query. ###
## functions for generating SQL to query data ##
def generate_sql_base_ms(db, schema, table):
    list_columns = stf.list_ms_columns(st.session_state.engine_ms, db, schema, table)
    str_columns = ', '.join(list_columns)
    sql = f'select  {str_columns}\nfrom  {db}.{schema}.{table}'
    st.session_state.sql_base_ms = sql

def update_sql_ms(sql):
    if st.session_state.updated_where_clause_ms.strip() != '':
        where_clause = f'\nwhere {st.session_state.updated_where_clause_ms}'
    else:
        where_clause = ''

    if st.session_state.updated_order_by_ms.strip() != '':
        order_by_clause = f'\norder by {st.session_state.updated_order_by_ms}'
    else:
        order_by_clause = ''

    if st.session_state.rows_to_limit_ms:
        sql_top = sql.replace('select', f'select top {str(st.session_state.rows_to_limit_ms)}')
    else:
        sql_top = sql

    updated_sql = sql_top + where_clause + order_by_clause
    st.session_state.sql_query_ms = updated_sql
    st.session_state.sql_text_area_updated_ms = updated_sql


## Use st.session_state to keep string values when widgets are updated ##
def update_rows_to_limit_ms(sql):
    st.session_state.rows_to_limit_ms = st.session_state.limit_rows_ms
    update_sql_ms(sql)

def update_where_clause_ms(sql):
    st.session_state.updated_where_clause_ms = st.session_state.where_clause_ms
    update_sql_ms(sql)

def update_order_clause_ms(sql):
    st.session_state.updated_order_by_ms = st.session_state.order_by_clause_ms
    update_sql_ms(sql)


### 3. When Execute SQL button  button is clicked, connect to snowflake and return result into dataframe display the df ###
## functions to reset display states and submit query ##
def reset_df_display_ms():
    st.session_state.display_df_ms = False
    st.session_state.display_entire_df_ms = False
    st.session_state.display_pandas_profile_ms = False
    st.session_state.pandas_profile_failed_ms = False
    st.session_state.df_converted_ms = False
    st.session_state.pandas_profile_complete_ms = False

def submit_query_ms():
    reset_df_display_ms()
    try:
        df = stf.return_ms_query_df(st.session_state.engine_ms, sql_text_area)
        st.session_state.sql_text_submitted_ms = sql_text_area
        st.session_state.df_ms = df
        st.session_state.display_df_ms = True
    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error: {e}')


## Execution of step #2 ##
if st.session_state.connected_to_ms:
    column_schema, column_table = st.columns(2)

    try:
        df_schemas = stf.list_ms_schemas(st.session_state.engine_ms, st.session_state.database_ms)
        schema_selected = column_schema.selectbox(
        "Select a schema:",
        df_schemas)

        df_tables = stf.list_ms_tables(st.session_state.engine_ms, st.session_state.database_ms, schema_selected)
        table_selected = column_table.selectbox(
        "Select a table:",
        df_tables)

        st.write("You selected:", st.session_state.database_ms + '.' + schema_selected + '.' + table_selected)
        generate_sql_base_ms(st.session_state.database_ms, schema_selected, table_selected)

        if 'rows_to_limit_ms' not in st.session_state:
            st.session_state.rows_to_limit_ms = 50000

        if 'updated_where_clause_ms' not in st.session_state:
            st.session_state.updated_where_clause_ms = ''

        if 'updated_order_by_ms' not in st.session_state:
            st.session_state.updated_order_by_ms = ''

        if 'sql_query_ms' not in st.session_state:
            # st.session_state.sql_query_ms = st.session_state.sql_base_ms + f'\nlimit {st.session_state.rows_to_limit_ms}'
            st.session_state.sql_query_ms = st.session_state.sql_base_ms.replace('select', f'select top {st.session_state.rows_to_limit_ms}')


        limit_rows_ms, where_clause_ms, order_by_clause_ms = st.columns(3)
        limit_rows_ms.number_input("limit rows:", value=st.session_state.rows_to_limit_ms, min_value=1, step=50000, key='limit_rows_ms', on_change=update_rows_to_limit_ms, args=[st.session_state.sql_base_ms])
        where_clause_ms.text_input("where:", value=st.session_state.updated_where_clause_ms, key='where_clause_ms',  on_change=update_where_clause_ms, args=[st.session_state.sql_base_ms])
        order_by_clause_ms.text_input("order by:", value=st.session_state.updated_order_by_ms, key='order_by_clause_ms',  on_change=update_order_clause_ms, args=[st.session_state.sql_base_ms])

        update_sql_ms(st.session_state.sql_base_ms)

        sql_text_area = st.text_area(
            label="SQL to send to Snowflake (Ctrl+Enter to update query manually):",
            key="sql_text_area_ms",
            value=st.session_state.sql_text_area_updated_ms,
            height=145
            )

        ## Preview of query to submit and submit button ##
        sq_preview, submit_query_button = st.columns([4, 1])
        sq_preview.write('Preview of query to submit: ' + sql_text_area[0:50] + ' ...... ' + sql_text_area[-50:])
        submit_query_button.button('Submit SQL to query data', on_click=submit_query_ms)

    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error: {e}')


### 4. Profile the dataframe using pandas profiling library or dtale library ###
## functions called to proflie dataframe when profile buttons are clicked ##
def profile_data_panda(df):
    st.session_state.display_pandas_profile_ms = True
    st.session_state.pandas_profile_failed_ms = False
    st.session_state.pandas_profile_complete_ms = False
    st.session_state.processing = True
    try:
        st.session_state.pr_ms = ProfileReport(df,
                                title=st.session_state.database_ms + '.' + schema_selected + '.' + table_selected,
                                minimal=True,
                                explorative=False,
                                correlations=None,
                                infer_dtypes=False,
                                vars={
                                    "num": {"low_categorical_threshold": 0},
                                    "cat": {
                                        "length": True,
                                        "characters": False,
                                        "words": False,
                                        "n_obs": 10,
                                    },
                                },
                                orange_mode=True)
    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error: {e}')
        st.session_state.processing = False


## save pandas profile report to output folder ##
def save_profile_report(pr):
    file_name = f'{database_name}-{st.session_state.database_ms}.{schema_selected}.{table_selected}-{cf.gvar.current_date_pst}.html'
    output_file_path = os.path.join(cf.gvar.path_outputs, file_name)

    try:
        pr.to_file(output_file_path)
        logger.info(f'Profile report saved to {output_file_path}')
        st.success(f'Profile report saved to {output_file_path}')
        # os.system(f'start {cf.gvar.path_outputs}') ## open output folder in windows but doesn't work in linux
    except Exception as e:
        logger.error(f'Error: {e}')
        st.error(f'Error: {e}')


## function for running dtale to profile data ##
def profile_data_dtale(df):
    if not st.session_state.dtale_running:
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
    st.session_state.df_converted_ms = True
    st.session_state.df_reverted_ms = False
    st.session_state.pandas_profile_failed_ms = False


## Revert back to original DataFrame if conversion was done ##
def revert_df_original():
    df = stf.return_ms_query_df(st.session_state.engine_ms, st.session_state.sql_text_submitted_ms)
    st.session_state.df_ms = df
    st.session_state.df_reverted_ms = True
    st.session_state.df_converted_ms = False


## display dataframe of queried data along with profile buttons ##
if st.session_state.display_df_ms:
    if st.session_state.display_entire_df_ms:
        st.write("All of queried data:")
        st.session_state.df_ms
    else:
        st.write("Displaying first 1000 rows of queried data:")
        st.session_state.df_ms[:1000]

    ## define streamlit objects and size of columns ##
    diplay_all_df, convert_df_button, gap1, pandas_profile_label, pandas_profile_button, output_profile_button, gap2, dtale_label, dtale_profile_button = st.columns([4, 3, 0.7, 2.3, 2, 2, 0.7, 2.3, 2])

    diplay_all_df.checkbox('Display all of queried data', key='display_entire_df', value=st.session_state.display_entire_df_ms)


## Display Pandas Profile report ##
if st.session_state.display_pandas_profile_ms:
    try:
        st_profile_report(st.session_state.pr_ms, navbar=True)
        st.session_state.pandas_profile_complete_ms = True
        st.session_state.processing = False
    except Exception as e:
        logger.error(f'Error while creating pandas profilng report')
        st.error(f'Error: {e}')
        st.session_state.display_pandas_profile_ms = False
        st.session_state.pandas_profile_failed_ms = True
        st.session_state.df_converted_ms = False
    st.session_state.processing = False


### buttons to toggle based on pandas profiling result ###
if st.session_state.display_df_ms:
    if not st.session_state.processing:

        ## buttons for pandas profiling ##
        pandas_profile_label.text('Pandas Profile:')

        if not st.session_state.pandas_profile_failed_ms:
            pandas_profile_button.button('Generate profile report', on_click=profile_data_panda, args=[st.session_state.df_ms])

        if not st.session_state.df_converted_ms:
            convert_df_button.button('Convert values to string (For Pandas Profile bugs)', on_click=convert_df_obj_to_str, args=[st.session_state.df_ms])
            if st.session_state.pandas_profile_failed_ms:
                pandas_profile_button.write('Profiling failed. Please convert object columns to string values to avoid errors.')

        if st.session_state.df_converted_ms and not st.session_state.df_reverted_ms:
            convert_df_button.button('Revert back to original DataFrame', on_click=revert_df_original)

        if st.session_state.pandas_profile_complete_ms and not st.session_state.pandas_profile_failed_ms:
            output_profile_button.button('Save profile report', on_click=save_profile_report, args=[st.session_state.pr_ms])

        ## buttons for dtale profiling ##
        dtale_label.text('Dtale Profile:')

        if not st.session_state.dtale_running:
            dtale_button_text = 'Run dtale to profile data'
        else:
            dtale_button_text = 'Stop running dtale'
        dtale_profile_button.button(dtale_button_text, on_click=profile_data_dtale, args=[st.session_state.df_ms])