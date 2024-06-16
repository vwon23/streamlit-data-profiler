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
database_name = 'ms_sql_server'
logger_name = f'{database_name}_data_profile'
logger = stf.st_initialize(path_app_run, logger_name)

st.markdown("# Microsoft SQL Server Data Profile")
st.write("This page uses SQL Alchemy library to connect to SQL Server to query data. Then queried data can be profiled using pandas profiling or dtale.")
st.divider()