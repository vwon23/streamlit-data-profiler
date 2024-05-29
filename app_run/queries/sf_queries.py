use_role_wh = """
    use role {sf_role};
    use warehouse {sf_wh};
"""

show_databases = """
    show databases;
"""

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