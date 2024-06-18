import streamlit as st
import os, sys
import utils.st_functions as stf

## Find path of the script then find the app_run path
path_script = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(path_script))

sys.path.append(path_app_run)


st.set_page_config(page_title="Home" ,page_icon="👋")

## define logger name and store global logger as st.session_state.logger to be used by other pages
logger_name = 'streamlit_data_profile'
st.session_state.logger = stf.st_initialize(path_app_run, logger_name)



st.write("# Welcome to Vince's Streamlit site! 👋")

st.markdown(
    """
    ###### This Streamlit app is devloped for profiling data from different types of databases.\n
    Source Code: https://github.com/vwon23/streamlit-data-profiler"""
)

st.divider()

st.markdown(
    """
    Streamlit is an open-source app framework built specifically for
    Machine Learning and Data Science projects using python.\n
    **👈 Select a demo from the sidebar** to see some examples
    of what Streamlit can do!
    ### Want to learn more?
    - Check out [streamlit.io](https://streamlit.io)
    - Jump into our [documentation](https://docs.streamlit.io)
    - Ask a question in our [community
        forums](https://discuss.streamlit.io)
    ### See more complex demos
    - Use a neural net to [analyze the Udacity Self-driving Car Image
        Dataset](https://github.com/streamlit/demo-self-driving)
    - Explore a [New York City rideshare dataset](https://github.com/streamlit/demo-uber-nyc-pickups)
"""
)

st.sidebar.success("Select a page above.")