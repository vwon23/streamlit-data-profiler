list_schemas = """
    select distinct
        table_schema
    from {db_name}.information_schema.tables;
"""

list_tables = """
    select distinct
        table_name
    from {db_name}.information_schema.tables
    where table_schema = '{schema}';
"""

get_columns = """
    select  ordinal_position,
            column_name
    from {db_name}.information_schema.columns
    where table_schema = '{schema}'
    and table_name = '{table}';
"""