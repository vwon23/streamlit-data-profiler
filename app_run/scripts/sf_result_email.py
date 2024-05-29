#### Import libraries and run initial scripts
import os, sys

## Find path of the script file then find the path of parent folder and add it to system path ##
path_file = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(path_file))
sys.path.append(path_app_run)

## import local modules
import utilities.common_functions as cf
import queries.sf_queries_dbt as sf_queries_dbt

## Initalize global variable and set logger ##
cf.init(path_app_run)
#cf.set_environment_variables()
cf.get_config()
cf.get_current_datetime()

current_file = os.path.basename(__file__)
loggername = current_file.split('.')[0]
logfile_name = f'{loggername}_{cf.gvar.current_date_pst}.log'
logger = cf.set_logger(loggername, logfile_name)

recipient = 'vwon87@gmail.com'


def main():
    ## Parse SQL to be exectued
    sql = sf_queries_dbt.failed_rule

    ## Connect to Snowflake
    cf.connect_snowflake_admin()

    ## Execute query and return result in df
    df = cf.sf_exec_query_return_df(sql)

    ## Send DF in an E-mail
    cf.send_email_df(recipient, df)


if __name__ == '__main__':
    main()