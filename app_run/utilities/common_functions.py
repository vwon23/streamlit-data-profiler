import sys, os
from dotenv import load_dotenv
import configparser
import logging, logging.config

import datetime as dt
from pytz import timezone
import re

import pandas as pd
import smtplib
import boto3
import pymysql
import snowflake.connector as sf
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# import streamlit as st

### - functions used for initializing global variables and logger - ###
## function to initialize global variables and logger ##
def initialize(path_app_run, loggername):
    set_gvar(path_app_run)
    get_config()
    get_current_datetime()

    logfile_name = f'{loggername}_{gvar.current_date_pst}.log'
    logger = set_logger(loggername, logfile_name)
    return logger

## set global variable and add path of app_run ##
def set_gvar(path_app_run):
    '''
    Creates global variable class gvar to handle the variables across scripts and functions. Sets the provided app_run path to gvar.path_app_run.

    Parameters
    ---------------
    path_app_run: path
        Directory path of app_run (e.g. app/app_run) returned from os.path.dir() function
    '''
    ## create a class to hold global variables ##
    class global_variables:
        variable_xyz = 'value'

    global gvar
    gvar = global_variables()
    gvar.path_app_run = path_app_run
    print(f'Application run path set as: {path_app_run}')


def set_env_from_dotenv():
    '''
    Sets os.environ values based on .env file located in parent folder of the app. (Only used for testing scripts locally)

    '''
    ## initalize environment variables
    dotenv_dir = os.path.dirname(os.path.dirname(gvar.path_app_run))
    dotenv_path =os.path.join(dotenv_dir, '.env')
    load_dotenv(dotenv_path=dotenv_path)


def get_config():
    '''
    Adds variables to global variable gvar based on values derived from environment variables and config.cfg file

    Parameters
    ---------------
    None
    '''
    config = configparser.ConfigParser()
    gvar.path_config = os.path.join(gvar.path_app_run, 'config')
    config.read(os.path.join(gvar.path_config, 'config.cfg'))

    gvar.run_local = config.get('ENVIRON', 'run_local')
    gvar.dotenv = config.get('ENVIRON', 'dotenv')

    ## function to derive sensitive data from environment variables or config.cfg file ##
    def config_from_env():
        ## Derive senetive data from environment variables (Recommended) ##
        gvar.env = os.environ['env']
        gvar.aws_rgn = os.environ['aws_rgn']

        gvar.mysql_hostname = os.environ['mysql_hostname']
        gvar.mysql_port = os.environ['mysql_port']
        gvar.mysql_username = os.environ['mysql_username']
        gvar.mysql_password = os.environ['mysql_password']

        gvar.sf_account = os.environ['snowflake_account']
        gvar.sf_username = os.environ['snowflake_username']
        gvar.sf_password = os.environ['snowflake_password']

        gvar.smtp_password = os.environ['smtp_password']

    def config_from_file():
        ## Derive senetive data from config.cfg file (Not recommended) ##
        gvar.env = config.get('ENVIRON', 'env')
        gvar.aws_rgn = config.get('AWS', 'aws_rgn')

        gvar.mysql_hostname = config.get('MYSQL', 'hostname')
        gvar.mysql_port = config.get('MYSQL', 'port')
        gvar.mysql_username = config.get('MYSQL', 'username')
        gvar.mysql_password = config.get('MYSQL', 'password')

        gvar.sf_account = config.get('SNOWFLAKE', 'account')
        gvar.sf_username = config.get('SNOWFLAKE', 'username')
        gvar.sf_password = config.get('SNOWFLAKE', 'password')

        gvar.smtp_password = config.get('EMAIL', 'password')

    ## Retreived senstive information choosing method based on confirguration
    if gvar.run_local != 'true':
        config_from_env()
    else:
        if gvar.dotenv != 'true':
            config_from_file()
        else:
            set_env_from_dotenv()
            config_from_env()

    ## path variables
    gvar.path_app = os.path.dirname(gvar.path_app_run)
    gvar.path_log = os.path.join(gvar.path_app, 'logs')
    gvar.path_logconfig = os.path.join(gvar.path_config, 'logging.cfg')
    gvar.path_outputs = os.path.join(gvar.path_app, 'outputs')

    ## create directories during run time
    if not os.path.exists(gvar.path_log):
        os.makedirs(gvar.path_log)
    if not os.path.exists(gvar.path_outputs):
        os.makedirs(gvar.path_outputs)

    ## mysql variables
    gvar.mysql_database = config.get('MYSQL', 'database')

    ## AWS variables
    gvar.aws_s3_bucket = config.get('AWS', 's3_bucket').format(env = gvar.env, aws_rgn = gvar.aws_rgn)
    gvar.aws_s3_bucket_name = gvar.aws_s3_bucket.split('//')[1]

    ## Snowflake variables
    gvar.sf_admin_role = config.get('SNOWFLAKE', 'admin_role')
    gvar.sf_admin_wh = config.get('SNOWFLAKE', 'admin_wh')

    gvar.sf_app_role = config.get('SNOWFLAKE', 'app_role')
    gvar.sf_app_wh = config.get('SNOWFLAKE', 'app_wh')
    gvar.sf_app_db = config.get('SNOWFLAKE', 'app_db')

    ## e-mail variables
    gvar.email_host = config.get('EMAIL', 'host')
    gvar.email_from = config.get('EMAIL', 'from')
    gvar.email_to = config.get('EMAIL', 'to')


def set_logger(loggername, filename):
    '''
    Sets logger based on selected loggername & Outputs to provided filename

    Parameters
    ---------------
    loggername: str
        The name of logger to set as. (The log name will be searched in logging.cfg to check config setting)
    filename: str
        the name to store logfile as

    Returns
    ---------------
    logger
        logger derived from logging.getLogger(loggername)
    '''

    gvar.path_logfile = os.path.join(gvar.path_log, filename)
    logging.config.fileConfig(gvar.path_logconfig, defaults={'logfilename': gvar.path_logfile})

    gvar.logger = logging.getLogger(loggername)
    global logger
    logger = logging.getLogger(__name__)
    logger.info(f'logs being written to {gvar.path_logfile}')

    return gvar.logger


###---  Datetime functions  ---###
def get_current_datetime():
    '''
    Sets variables for current date/time values.

    Parameters
    ---------------
    None
    '''
    ## UTC Time variables ##
    gvar.current_utc = dt.datetime.now()
    gvar.current_datetime_utc = gvar.current_utc.strftime("%Y-%m-%d %H:%M:%S")

    ## PST Time variables
    gvar.current_pst = dt.datetime.now().astimezone(timezone('US/Pacific'))
    gvar.current_year_pst = gvar.current_pst.strftime("%Y")
    gvar.current_date_pst = gvar.current_pst.strftime("%Y-%m-%d")
    gvar.current_datetime_pst = gvar.current_pst.strftime("%Y-%m-%d %H:%M:%S")

    print(f'Current Time in PST: {gvar.current_datetime_pst}')


def convert_timestmp_int(timestmp_int):
    '''
    Converts timestamp value in integer to string.

    Parameters
    ---------------
    timestmp_int: int
        timestamp value in integer

    Returns
    ---------------
    timestmp_str: str
        timestamp value in str
    '''
    timestmp_pst_str = dt.datetime.fromtimestamp(timestmp_int).astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    return timestmp_pst_str


###---  Database functions  ---###
def connect_mysql():
    '''
    Creates connection to mysql and returns connection

    Parameters
    ---------------
    None

    Returns
    ---------------
    conn
        connection returned using function pymysql.connect
    '''

    conn = pymysql.connect(host=gvar.mysql_hostname,
                            user=gvar.mysql_username,
                            password=gvar.mysql_password,
                            db=gvar.mysql_database,
                            port=int(gvar.mysql_port)
                            )
    if conn is None:
        logger.error(f'Error connecting to the MySQL database {gvar.mysql_hostname}')
    else:
        logger.info(f'MySQL connection established to {gvar.mysql_hostname}')
    return conn



###--- Snowflake functions ---###
def connect_snowflake_sso(sf_user, sf_role, sf_wh):
    '''
    Creates connection to snowflake and curor using application cred and sets it to gvar.sf_conn and gvar.sf_cur

    Parameters
    ---------------
    None

    Returns
    ---------------
    None

    '''
    try:
        if not hasattr(gvar, 'sf_cursor'):
            gvar.sf_conn = sf.connect(
                                    account=gvar.sf_account,
                                    user=sf_user,
                                    authenticator='externalbrowser',
                                    role=sf_role,
                                    warehouse=sf_wh
                                    )
            gvar.sf_cursor = gvar.sf_conn.cursor()
        logger.info(f'Successfully connected to Snowflake account {gvar.sf_account}')
    except Exception as e:
        logger.error(f'Error connecting to Snowflake account {gvar.sf_account}\n' + f'{e}')




def connect_snowflake_login(sf_user, sf_role, sf_wh):
    '''
    Creates connection to snowflake and curor using application cred and sets it to gvar.sf_conn and gvar.sf_cur

    Parameters
    ---------------
    None

    Returns
    ---------------
    None

    '''
    try:
        if not hasattr(gvar, 'sf_cursor'):
            gvar.sf_conn = sf.connect(
                                    account=gvar.sf_account,
                                    user=sf_user,
                                    password=gvar.sf_password,
                                    role=sf_role,
                                    warehouse=sf_wh
                                    )
            gvar.sf_cursor = gvar.sf_conn.cursor()
        logger.info(f'Successfully connected to Snowflake account {gvar.sf_account}')
    except Exception as e:
        logger.error(f'Error connecting to Snowflake account {gvar.sf_account}\n' + f'{e}')


def connect_snowflake_admin():
    '''
    Creates connection to snowflake and curor using default adming ROLE and WH. Sets connections and cursor to gvar.sf_conn and gvar.sf_cur.

    Parameters
    ---------------
    None

    Returns
    ---------------
    None

    '''
    try:
        if not hasattr(gvar, 'sf_cursor'):
            gvar.sf_conn = sf.connect(
                                    account=gvar.sf_account,
                                    user=gvar.sf_username,
                                    password=gvar.sf_password,
                                    role=gvar.sf_admin_role,
                                    warehouse=gvar.sf_admin_wh
                                    )
            gvar.sf_cursor = gvar.sf_conn.cursor()
        logger.info(f'Successfully connected to Snowflake account {gvar.sf_account}')
    except Exception as e:
        logger.error(f'Error connecting to Snowflake account {gvar.sf_account}\n' + f'{e}')


def connect_snowflake_app():
    '''
    Creates connection to snowflake and curor using application cred and sets it to gvar.sf_conn and gvar.sf_cur

    Parameters
    ---------------
    None

    Returns
    ---------------
    None

    '''
    try:
        if not hasattr(gvar, 'sf_cursor'):
            gvar.sf_conn = sf.connect(
                                    account=gvar.sf_account,
                                    user=gvar.sf_username,
                                    password=gvar.sf_password,
                                    role=gvar.sf_app_role,
                                    warehouse=gvar.sf_app_wh,
                                    database=gvar.sf_app_db
                                    )
            gvar.sf_cursor = gvar.sf_conn.cursor()
        logger.info(f'Successfully connected to Snowflake account {gvar.sf_account}')
    except Exception as e:
        logger.error(f'Error connecting to Snowflake account {gvar.sf_account}\n' + f'{e}')


def sf_exec_query_return_df(query):
    '''
    Sets DictCursor and executes query and returns result in dataframe.

    Parameters
    ---------------
    query: str

    Returns
    ---------------
    Pandas DataFrame

    '''
    query_striped = query.strip()
    logger.info('Executing query:\n' + query_striped)

    try:
        sf_dict_cursor = gvar.sf_conn.cursor(sf.DictCursor)
        result = sf_dict_cursor.execute(query).fetchall()
    except Exception as e:
        logger.error(f'Error executing query to Snowflake account {gvar.sf_account}\n' + f'{e}')



    result_df = pd.DataFrame(result)
    logger.info('Stored result of executed query into DataFrame successfully')

    return result_df





###---  AWS functions   ---###
def s3_upload_file(file_path, bucket_name, key):
    '''
    uploads file to aws s3 bucket

    Parameters
    ---------------
    file_path: str
        The path of file to upload
    bucket_name: str
        The name of aws s3 bucket
    key: str
        The key value to upload file as
    '''

    s3c = boto3.client('s3')
    try:
        s3c.upload_file(file_path, bucket_name, key)
    except:
        logger.error(f'Error occured while uploading {file_path} to aws s3 bucket {bucket_name}' + '\n')
    else:
        logger.info(f'Successfully uploaded {file_path} to aws s3 bucket {bucket_name} as {key}' + '\n')


def s3_upload_log(file_path, bucket_name, logfile_name):
    '''
    uploads logfile to aws s3 bucket/logs folder

    '''

    bucket_path_logs = 'logs'
    bucket_logfile_key = f'{bucket_path_logs}/{logfile_name}'
    s3_upload_file(file_path, bucket_name, bucket_logfile_key)



def s3_clean_bucket(bucket_name, prefix, n=365):
    '''
    Deletes objects older than n days inside s3 bucket. Filters objects based on prefix

    Parameters
    ---------------
    bucket_name: str
        The name of aws s3 bucket
    prefix: str
        String prefix to filter objects
    n: int
        The number of days older than current date to remove objects
    '''

    s3r = boto3.resource('s3')
    s3_bucket = s3r.Bucket(bucket_name)

    logger.info(f'Removing objects older than {n} days inside s3 bucket {bucket_name} with prefix {prefix}')
    for obj in s3_bucket.objects.filter(Prefix=prefix):
        if obj.last_modified < gvar.current_pst - dt.timedelta(days=n):
            logger.info(f'{obj.key} is older than {n} days and is deleted')
            obj.delete()



###---  E-mail functions   ---###
def send_email(mail_to, mail_msg):
    '''
    Sets up sftp for email and sends e-mail.

    Parameters
    ---------------
    mail_to: str
        E-mail address(s) to send E-mail to
    mail_msg: str
        Content of E-mail

    Returns
    ---------------
    None
    '''

    host = gvar.email_host
    #recipients = mail_to.split(";")
    recipients = re.split('; |, |\*|\n', mail_to)

    server = smtplib.SMTP(host, 25)
    try:
        server.connect(host, 25)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(gvar.email_from, gvar.smtp_password)
        server.sendmail(gvar.email_from, recipients, mail_msg)
        logger.info('E-mail sent successfully to {recipients}')
    except Exception as e:
        logger.error({e})
    server.close()


def send_email_df(mail_to, df):
    '''
    Sets up sftp for email and sends DataFrame in an e-mail.

    Parameters
    ---------------
    mail_to: str
        E-mail address(s) to send E-mail to
    df: str
        DataFrame to send E-mail as

    Returns
    ---------------
    None
    '''
    host = gvar.email_host
    #recipients = mail_to.split(";")
    recipients = re.split('; |, |\*|\n', mail_to)

    html_table = df.to_html(index=False)

    html_content = f"""
    <html>
    <body>
        <p>Hi,<br>
        Please find the DataFrame below:<br>
        </p>
        {html_table}
    </body>
    </html>
    """

    message = MIMEMultipart("alternative")
    message['From'] = gvar.email_from
    message['To'] = '; '.join(map(str, recipients))
    message['Subject'] = 'Pandas DataFrame in HTML Format'

    part = MIMEText(html_content, "html")
    message.attach(part)

    server = smtplib.SMTP(host, 25)

    try:
        server.connect(host, 25)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(gvar.email_from, gvar.smtp_password)
        server.sendmail(gvar.email_from, recipients, message.as_string())
        logger.info('E-mail sent successfully to {recipients}')
    except Exception as e:
        logger.error({e})

    server.close()


# #### functions used for Streamlit ####
# @st.cache_data
# def st_initialize(path_app_run, loggername):
#     st_logger = initialize(path_app_run, loggername)
#     return st_logger