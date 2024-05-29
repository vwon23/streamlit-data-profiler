#### Import libraries and run initial scripts
import os, sys

## Find path of the script file then find the path of parent folder and add it to system path ##
path_file = os.path.abspath(__file__)
path_app_run = os.path.dirname(os.path.dirname(path_file))
sys.path.append(path_app_run)

## import local modules
import utilities.common_functions as cf
import queries.sf_queries_admin as sf_queries_admin

## Initalize global variable and set logger ##
cf.init(path_app_run)
#cf.set_environment_variables()
cf.get_config()
cf.get_current_datetime()

current_file = os.path.basename(__file__)
loggername = current_file.split('.')[0]
logfile_name = f'{loggername}_{cf.gvar.current_date_pst}.log'
logger = cf.set_logger(loggername, logfile_name)


def main():
    #### Parse SQL to be exectued
    create_environ_sql = sf_queries_admin.create_environ.format(SF_ADMIN_ROLE=cf.gvar.sf_admin_role,
                                                        SF_DATABASE=cf.gvar.sf_app_db,
                                                        SF_ROLE=cf.gvar.sf_app_role,
                                                        SF_WH=cf.gvar.sf_app_wh)

    remove_environ_sql = sf_queries_admin.remove_environ.format(SF_ADMIN_ROLE=cf.gvar.sf_admin_role,
                                                        SF_DATABASE=cf.gvar.sf_app_db,
                                                        SF_ROLE=cf.gvar.sf_app_role,
                                                        SF_WH=cf.gvar.sf_app_wh)

    #sql = create_environ_sql
    sql = remove_environ_sql
    logger.info(f'SQL to be executed:\n {sql}')

    #### Connect to Snowflake
    cf.connect_snowflake_admin()
    # cf.connect_snowflake()

    #### Execute SQL and loop through results
    cursor_list = cf.gvar.sf_conn.execute_string(sql)

    for cursor in cursor_list:
        for row in cursor:
            logger.info(row[0])


if __name__ == '__main__':
    main()