create_db_schema = """
    create warehouse transforming;
    create database raw;
    create database analytics;
    create schema raw.jaffle_shop;
    create schema raw.stripe;
"""

create_table_customers = """
    create table raw.jaffle_shop.customers
    (   id integer,
        first_name varchar,
        last_name varchar,
        _etl_loaded_at timestamp default current_timestamp
    );
"""

load_table_customers = """
    copy into raw.jaffle_shop.customers (id, first_name, last_name)
    from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
    file_format = (
        type = 'CSV'
        field_delimiter = ','
        skip_header = 1
        );
"""

create_table_orders = """
    create table raw.jaffle_shop.orders
    (   id integer,
        user_id integer,
        order_date date,
        status varchar,
        _etl_loaded_at timestamp default current_timestamp
    );
"""

load_table_orders = """
    copy into raw.jaffle_shop.orders (id, user_id, order_date, status)
    from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
    file_format = (
        type = 'CSV'
        field_delimiter = ','
        skip_header = 1
        );
"""

create_table_payment = """
    create table raw.stripe.payment
    (   id integer,
        orderid integer,
        paymentmethod varchar,
        status varchar,
        amount integer,
        created date,
        _etl_loaded_at timestamp default current_timestamp
    );
"""

load_table_payment = """
    copy into raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
    from 's3://dbt-tutorial-public/stripe_payments.csv'
    file_format = (
        type = 'CSV'
        field_delimiter = ','
        skip_header = 1
        );
"""

drop_wh_dbs = """
    drop warehouse transforming;
    drop database raw;
    drop database analytics;
"""

failed_rule = """
    with payments as (
        select * from analytics.vwon.stg_payments
    )

    select
        order_id,
        sum(amount) as total_amount
    from payments

    group by order_id
    having total_amount > 50
"""