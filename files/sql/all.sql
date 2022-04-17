CREATE TYPE EARN_KEY AS STRUCT<CUSTOMER_ID INT, CAMPAIGN_ID INT, TERM STRING>;

CREATE TABLE customer {
    customer_id INT PRIMARY KEY,
    is_employee BOOLEAN,
    business_line STRING,
    customer_since TIMESTAMP,
    onboarding_channel STRING
}

CREATE STREAM debit_card_transactions (
    txnid INT KEY,
    customer_id INT,
    amount DECIMAL(12,2),
    merchant_id INT,
    transaction_time STRING
)
WITH (
        KAFKA_TOPIC='debit_card_transactions',
        PARTITIONS='4',
        VALUE_FORMAT='JSON',
        TIMESTAMP='transaction_time',
        TIMESTAMP_FORMAT='yyyy-MM-dd HH:mm:ss'
);

INSERT INTO debit_card_transactions VALUES(1,1,10.00,1,'2022-04-01 12:23:14');
INSERT INTO debit_card_transactions VALUES(7,1,20.00,1,'2022-04-07 21:12:53');
INSERT INTO debit_card_transactions VALUES(23,1,10.00,1,'2022-04-09 11:53:43');
INSERT INTO debit_card_transactions VALUES(39,1,80.00,1,'2022-04-11 17:53:43');
INSERT INTO debit_card_transactions VALUES(121,1,80.00,1,'2022-04-18 19:25:08');



CREATE TABLE cashback_earned 
    WITH (
        KAFKA_TOPIC='cashback_earned',
        PARTITIONS='4',
        KEY_FORMAT='JSON',
        VALUE_FORMAT='JSON'
    )   
    AS SELECT 
    STRUCT(customer_id := customer_id,campaign_id := 1, term := SUBSTRING(transaction_time,1,7)) AS earn_id,
    CAST(LEAST(sum(amount)/2,50.00) AS DECIMAL(12,2) AS amount
    FROM debit_card_transactions
    WHERE merchant_id = 1
    GROUP BY STRUCT(customer_id := customer_id,campaign_id := 1, term := SUBSTRING(transaction_time,1,7)) EMIT CHANGES;

CREATE STREAM cashback_order (
        earn_id EARN_KEY KEY,
        amount DECIMAL(12,2)
    ) WITH (
        KAFKA_TOPIC='cashback_order',
        PARTITIONS='4',
        VALUE_FORMAT='JSON',
        KEY_FORMAT='JSON'
);

CREATE TABLE total_cashback_order 
WITH (
    KAFKA_TOPIC = 'total_cashback_order',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
    )
AS SELECT 
        earn_id,
        sum(amount) AS total_amount
    FROM cashback_order
    GROUP BY earn_id
    EMIT CHANGES;

CREATE STREAM earned_order_diff_stream (earn_id EARN_KEY KEY,amount DECIMAL(12,2))
WITH (
    KAFKA_TOPIC = 'earned_order_diff',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON'
    );

CREATE TABLE earned_order_diff 
WITH (
    KAFKA_TOPIC = 'earned_order_diff',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
    )
AS
    SELECT 
        c.earn_id,
        CAST(c.amount - IFNULL(t.total_amount,CAST(0.00 AS DECIMAL(12,2))) AS DECIMAL(12,2)) as amount 
    FROM cashback_earned c 
    LEFT JOIN   total_cashback_order t  on c.earn_id = t.earn_id
    EMIT CHANGES;

INSERT INTO cashback_order select * from earned_order_diff_stream where abs(amount) > 0.1 emit changes;
