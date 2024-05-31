create_environ = """
    use role {SF_ADMIN_ROLE};

    create database {SF_DATABASE};
    create role {SF_ROLE};
    create OR replace warehouse {SF_WH}
        with WAREHOUSE_SIZE= XSMALL;

    grant all on database {SF_DATABASE} to role {SF_ROLE};
    grant all on future tables in database {SF_DATABASE} to role {SF_ROLE};
    grant all on future schemas in database {SF_DATABASE} to role {SF_ROLE};

    grant usage on warehouse {SF_WH} to role {SF_ROLE};
    grant role {SF_ROLE} to role {SF_ADMIN_ROLE};
"""

remove_environ = """
    use role {SF_ADMIN_ROLE};

    revoke role {SF_ROLE} from role {SF_ADMIN_ROLE};
    revoke usage on warehouse {SF_WH} from role {SF_ROLE};

    revoke all on future schemas in database {SF_DATABASE} from role {SF_ROLE};
    revoke all on future tables in database {SF_DATABASE} from role {SF_ROLE};
    revoke all on database {SF_DATABASE} from role {SF_ROLE};

    drop warehouse {SF_WH};
    drop role {SF_ROLE};
    drop database {SF_DATABASE};
"""