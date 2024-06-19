list_dbs = """
    SELECT distinct name
    FROM sys.databases
    WHERE name not in ('master','tempdb','model','msdb');
"""

list_schemas = """
    SELECT distinct
        table_schema
    FROM {db_name}.information_schema.tables;
"""

list_tables = """
    SELECT distinct
        table_name
    FROM {db_name}.information_schema.tables
    WHERE table_schema = '{schema}';
"""

get_columns = """
    SELECT  ordinal_position,
            column_name
    FROM {db_name}.information_schema.columns
    WHERE table_schema = '{schema}'
    AND table_name = '{table}';
"""