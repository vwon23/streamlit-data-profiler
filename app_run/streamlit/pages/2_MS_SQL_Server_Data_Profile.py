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
    st.session_state.connected_to_sf = False

if 'display_df_ms' not in st.session_state:
    st.session_state.display_df_sf = False

if 'display_entire_df_ms' not in st.session_state:
    st.session_state.display_entire_df_sf = False

if 'display_pandas_profile_ms' not in st.session_state:
    st.session_state.display_pandas_profile_sf = False

if 'pandas_profile_failed_ms' not in st.session_state:
    st.session_state.pandas_profile_failed_sf = False

if 'df_converted_ms' not in st.session_state:
    st.session_state.df_converted_sf = False

if 'df_reverted_ms' not in st.session_state:
    st.session_state.df_reverted_sf = False

if 'dtale_running' not in st.session_state:
    st.session_state.dtale_running = False

if 'processing' not in st.session_state:
    st.session_state.processing = False


### 1. Connect to Snowflake when "Connect to Snowflake" button is clicked. ###
## Connect to snowflake and set the session state to False to prevent reconnection ##
@st.cache_data
def connect_to_mssql(server, database, windows_logon):
    print('TODO')
    ## TODO: Add SSO connection to MS SQL Server
    # try:
    #     if sso:
    #         cf.connect_snowflake_sso(sf_user, sf_role, sf_wh)
    #     else:
    #         cf.connect_snowflake_login(sf_user, sf_role, sf_wh)

    #     st.session_state.sf_user = sf_user
    #     st.session_state.sf_role = sf_role
    #     st.session_state.sf_wh = sf_wh
    #     st.session_state.connected_to_sf = True
    # except Exception as e:
    #     logger.error(f'Error: {e}')
    #     st.error(f'Error connecting to Snowflake: {e}')

## Form to enter login information to connect to Snowflake ##
with st.sidebar.form('mssql_connection_form'):
    st.header("SQL Server Login")

    server_input = st.text_input("Server:", cf.gvar.sf_username)
    database_input = st.text_input("Database:", cf.gvar.sf_app_role)

    windows_logon_checkbox = st.checkbox("Use Windows Auth", value=True)
    connect_button = st.form_submit_button('Connect to SQL Server')

    if connect_button:
        connect_to_mssql(server_input, database_input, windows_logon_checkbox)




# ### 2. If connected to snowflake, show databases, schemas, tables and generated SQL to query. ###
# ## functions for generating SQL to query data ##
# def generate_sql_base(db, schema, table):
#     list_columns = stf.list_sf_columns(db, schema, table)
#     str_columns = ', '.join(list_columns)
#     sql = f'select  {str_columns}\nfrom  {db}.{schema}.{table}'
#     st.session_state.sql_base = sql

# def update_sql(sql):
#     if st.session_state.updated_where_clause.strip() != '':
#         where_clause = f'\nwhere {st.session_state.updated_where_clause}'
#     else:
#         where_clause = ''

#     if st.session_state.updated_order_by.strip() != '':
#         order_by_clause = f'\norder by {st.session_state.updated_order_by}'
#     else:
#         order_by_clause = ''

#     if st.session_state.rows_to_limit:
#         limit_rows_clause = f'\nlimit {str(st.session_state.rows_to_limit)}'
#     else:
#         limit_rows_clause = ''

#     updated_sql = sql + where_clause + order_by_clause + limit_rows_clause
#     st.session_state.sql_query = updated_sql
#     st.session_state.sql_text_area_updated = updated_sql


# ## Use st.session_state to keep string values when widgets are updated ##
# def update_rows_to_limit(sql):
#     st.session_state.rows_to_limit = st.session_state.limit_rows
#     update_sql(sql)

# def update_where_clause(sql):
#     st.session_state.updated_where_clause = st.session_state.where_clause
#     update_sql(sql)

# def update_order_clause(sql):
#     st.session_state.updated_order_by = st.session_state.order_by_clause
#     update_sql(sql)


# ### 3. When Execute SQL button  button is clicked, connect to snowflake and return result into dataframe display the df ###
# ## functions to reset display states and submit query ##
# def reset_df_display():
#     st.session_state.display_df_sf = False
#     st.session_state.display_entire_df_sf = False
#     st.session_state.display_pandas_profile_sf = False
#     st.session_state.pandas_profile_failed_sf = False
#     st.session_state.df_converted_sf = False
#     st.session_state.pandas_profile_complete_sf = False
#     st.session_state.dtale_running = False

# def submit_query():
#     reset_df_display()
#     try:
#         df = stf.return_sf_query_df(sql_text_area)
#         st.session_state.sql_text_submitted = sql_text_area
#         st.session_state.df_sf = df
#         st.session_state.display_df_sf = True
#     except Exception as e:
#         logger.error(f'Error: {e}')
#         st.error(f'Error: {e}')


# ## Execution of step #2 ##
# if st.session_state.connected_to_sf:
#     column_db, column_schema, column_table = st.columns(3)

#     try:
#         list_dbs = stf.list_sf_databases(st.session_state.sf_role, st.session_state.sf_wh)
#         db_selected = column_db.selectbox(
#         "Select a database:",
#         list_dbs)

#         df_schemas = stf.list_sf_schemas(db_selected)
#         schema_selected = column_schema.selectbox(
#         "Select a schema:",
#         df_schemas)

#         df_tables = stf.list_sf_tables(db_selected, schema_selected)
#         table_selected = column_table.selectbox(
#         "Select a table:",
#         df_tables)

#         st.write("You selected:", db_selected + '.' + schema_selected + '.' + table_selected)
#         generate_sql_base(db_selected, schema_selected, table_selected)

#         if 'rows_to_limit' not in st.session_state:
#             st.session_state.rows_to_limit = 100000

#         if 'updated_where_clause' not in st.session_state:
#             st.session_state.updated_where_clause = ''

#         if 'updated_order_by' not in st.session_state:
#             st.session_state.updated_order_by = ''

#         if 'sql_query' not in st.session_state:
#             st.session_state.sql_query = st.session_state.sql_base + f'\nlimit {st.session_state.rows_to_limit}'


#         limit_rows, where_clause, order_by_clause = st.columns(3)
#         limit_rows.number_input("limit rows:", value=st.session_state.rows_to_limit, min_value=1, step=50000, key='limit_rows', on_change=update_rows_to_limit, args=[st.session_state.sql_base])
#         where_clause.text_input("where:", value=st.session_state.updated_where_clause, key='where_clause',  on_change=update_where_clause, args=[st.session_state.sql_base])
#         order_by_clause.text_input("order by:", value=st.session_state.updated_order_by, key='order_by_clause',  on_change=update_order_clause, args=[st.session_state.sql_base])

#         update_sql(st.session_state.sql_base)

#         sql_text_area = st.text_area(
#             label="SQL to send to Snowflake (Ctrl+Enter to update query manually):",
#             key="sql_text_area",
#             value=st.session_state.sql_text_area_updated,
#             height=145
#             )

#         ## Preview of query to submit and submit button ##
#         sq_preview, submit_query_button = st.columns([4, 1])
#         sq_preview.write('Preview of query to submit: ' + sql_text_area[0:50] + ' ...... ' + sql_text_area[-50:])
#         submit_query_button.button('Submit SQL to query data', on_click=submit_query)

#     except Exception as e:
#         logger.error(f'Error: {e}')
#         st.error(f'Error: {e}')


# ### 4. Profile the dataframe using pandas profiling library or dtale library ###
# ## functions called to proflie dataframe when profile buttons are clicked ##
# def profile_data_panda(df):
#     st.session_state.display_pandas_profile_sf = True
#     st.session_state.pandas_profile_failed_sf = False
#     st.session_state.pandas_profile_complete_sf = False
#     st.session_state.processing = True
#     try:
#         st.session_state.pr_sf = ProfileReport(df,
#                                 title=db_selected + '.' + schema_selected + '.' + table_selected,
#                                 minimal=True,
#                                 explorative=False,
#                                 correlations=None,
#                                 infer_dtypes=False,
#                                 vars={
#                                     "num": {"low_categorical_threshold": 0},
#                                     "cat": {
#                                         "length": True,
#                                         "characters": False,
#                                         "words": False,
#                                         "n_obs": 10,
#                                     },
#                                 },
#                                 orange_mode=True)
#     except Exception as e:
#         logger.error(f'Error: {e}')
#         st.error(f'Error: {e}')
#         st.session_state.processing = False


# ## save pandas profile report to output folder ##
# def save_profile_report(pr):
#     file_name = f'{database_name}-{db_selected}.{schema_selected}.{table_selected}-{cf.gvar.current_date_pst}.html'
#     output_file_path = os.path.join(cf.gvar.path_outputs, file_name)

#     try:
#         pr.to_file(output_file_path)
#         logger.info(f'Profile report saved to {output_file_path}')
#         st.success(f'Profile report saved to {output_file_path}')
#         # os.system(f'start {cf.gvar.path_outputs}') ## open output folder in windows but doesn't work in linux
#     except Exception as e:
#         logger.error(f'Error: {e}')
#         st.error(f'Error: {e}')


# ## function for running dtale to profile data ##
# def profile_data_dtale(df):
#     if not st.session_state.dtale_running:
#         st.session_state.dtale = dtale.show(df, host='localhost')
#         st.session_state.dtale.open_browser()
#         st.session_state.dtale_running = True
#     else:
#         st.session_state.dtale.kill()
#         st.session_state.dtale_running = False


# ## Convert object columns to string values to avoid pandas profiling errors ##
# def convert_df_obj_to_str(df):
#     for col in df.columns:
#         if df[col].dtype.name == 'object':
#             df[col] = df[col].astype(str)
#     logger.info('Pandas Dataframe Object columns converted to string values')
#     st.session_state.df_converted_sf = True
#     st.session_state.df_reverted_sf = False
#     st.session_state.pandas_profile_failed_sf = False


# ## Revert back to original DataFrame if conversion was done ##
# def revert_df_original():
#     df = stf.return_sf_query_df(st.session_state.sql_text_submitted)
#     st.session_state.df_sf = df
#     st.session_state.df_reverted_sf = True
#     st.session_state.df_converted_sf = False


# ## display dataframe of queried data along with profile buttons ##
# if st.session_state.display_df_sf:
#     if st.session_state.display_entire_df_sf:
#         st.write("All of queried data:")
#         st.session_state.df_sf
#     else:
#         st.write("Displaying first 1000 rows of queried data:")
#         st.session_state.df_sf[:1000]

#     ## define streamlit objects and size of columns ##
#     diplay_all_df, convert_df_button, gap1, pandas_profile_label, pandas_profile_button, output_profile_button, gap2, dtale_label, dtale_profile_button = st.columns([4, 3, 0.7, 2.3, 2, 2, 0.7, 2.3, 2])

#     diplay_all_df.checkbox('Display all of queried data', key='display_entire_df', value=st.session_state.display_entire_df_sf)


# ## Display Pandas Profile report ##
# if st.session_state.display_pandas_profile_sf:
#     try:
#         st_profile_report(st.session_state.pr_sf, navbar=True)
#         st.session_state.pandas_profile_complete_sf = True
#         st.session_state.processing = False
#     except Exception as e:
#         logger.error(f'Error while creating pandas profilng report')
#         st.error(f'Error: {e}')
#         st.session_state.display_pandas_profile_sf = False
#         st.session_state.pandas_profile_failed_sf = True
#         st.session_state.df_converted_sf = False
#     st.session_state.processing = False


# ### buttons to toggle based on pandas profiling result ###
# if st.session_state.display_df_sf:
#     if not st.session_state.processing:

#         ## buttons for pandas profiling ##
#         pandas_profile_label.text('Pandas Profile:')

#         if not st.session_state.pandas_profile_failed_sf:
#             pandas_profile_button.button('Generate profile report', on_click=profile_data_panda, args=[st.session_state.df_sf])

#         if not st.session_state.df_converted_sf:
#             convert_df_button.button('Convert values to string (For Pandas Profile bugs)', on_click=convert_df_obj_to_str, args=[st.session_state.df_sf])
#             if st.session_state.pandas_profile_failed_sf:
#                 pandas_profile_button.write('Profiling failed. Please convert object columns to string values to avoid errors.')

#         if st.session_state.df_converted_sf and not st.session_state.df_reverted_sf:
#             convert_df_button.button('Revert back to original DataFrame', on_click=revert_df_original)

#         if st.session_state.pandas_profile_complete_sf and not st.session_state.pandas_profile_failed_sf:
#             output_profile_button.button('Save profile report', on_click=save_profile_report, args=[st.session_state.pr_sf])

#         ## buttons for dtale profiling ##
#         dtale_label.text('Dtale Profile:')

#         if not st.session_state.dtale_running:
#             dtale_button_text = 'Run dtale to profile data'
#         else:
#             dtale_button_text = 'Stop running dtale'
#         dtale_profile_button.button(dtale_button_text, on_click=profile_data_dtale, args=[st.session_state.df_sf])