from pyspark.sql.functions import *

from dependencies.spark import start_spark


def main():
    """
    Main code for processing Batch Data pipeline
    :return: None
    """

    spark, logs = start_spark(app_name='batch_job')

    logs.info('Batch job is up-and-running')

    # Reading the input files and registering temporary tables required for Batch processing
    read_input(spark, data_format='csv', input_type='card')
    read_input(spark, data_format='csv', input_type='card_instance')
    read_input(spark, data_format='csv', input_type='card_instance_shipping')
    read_input(spark, data_format='csv', input_type='currency')
    read_input(spark, data_format="csv", input_type="transactions")

    run_batch_job(spark)

    logs.info('Batch job is finished')
    spark.stop()
    return None


def read_input(spark, data_format="csv", input_type="card"):
    """
    Read the input file, create Dataframe and register as temporary view
    :param spark: Spark session
    :param data_format: format of input file
    :param input_type: type of input file
    :return: None
    """

    df = spark.read.format(data_format).options(header=True).load("data/input/" + input_type + "." + data_format)
    df.createOrReplaceTempView(input_type)

    return input_type


def run_batch_job(spark):
    """
    Run the Batch specific queries on top of temporary view created
    :param spark: Spark session
    :param spark: Spark session
    :return: None
    """

    card_details_df = spark.sql("""SELECT c.id AS Card,
        c.creation_date    AS orderdate,
        cs.creation_date   AS issuedate,
        ci.activation_date AS activationdate,
        Min(ct.tx_date)    AS firstuseddate
        FROM  card c
        LEFT OUTER JOIN card_instance ci
            ON ( c.id = ci.card_id )
        LEFT OUTER JOIN card_instance_shipping cs
            ON ( ci.id = cs.card_instance_id )
        LEFT OUTER JOIN transactions ct
            ON ( c.id = ct.card_id )
        GROUP  BY c.id,
            c.creation_date,
            cs.creation_date,
            ci.activation_date""")

    card_details_df.createOrReplaceTempView("card_details")

    # !!!Coalesce(1) is not a good practise in Production
    card_details_df.coalesce(1).write.format("csv").options(header=True).save("data/output/card_details")

    card_activated_firstuse_df = spark.sql("""WITH cleansedcarddetails
        AS (SELECT card,
                issuedate,
                activationdate,
                firstuseddate
        FROM   card_details
        WHERE  activationdate IS NOT NULL
                AND firstuseddate IS NOT NULL)
        SELECT card,
            issuedate,
            activationdate,
            firstuseddate
        FROM  cleansedcarddetails
        WHERE Datediff(To_timestamp(activationdate, 'yyyy-MM-dd HH:mm:ss'),
              To_timestamp(issuedate, 'yyyy-MM-dd HH:mm:ss')) > 3
        AND Datediff(To_timestamp(firstuseddate, 'yyyy-MM-dd HH:mm:ss'),
               To_timestamp(activationdate, 'yyyy-MM-dd HH:mm:ss')) > 10""")

    card_activated_firstuse_df.coalesce(1).write.format("csv").options(header=True).save("data/output/card_activated_firstuse")

    cancelled_cards_df = spark.sql("""SELECT card_id AS card,
        activation_date,
        deactivation_date, Datediff(To_timestamp(deactivation_date,
        'yyyy-MM-dd HH:mm:ss'), To_timestamp(activation_date, 'yyyy-MM-dd HH:mm:ss')) AS
        activedays
        FROM card_instance
        WHERE deactivation_date IS NOT NULL AND activation_date IS NOT NULL""")

    cancelled_cards_df.coalesce(1).write.format("csv").options(header=True).save("data/output/cancelled_cards")

    merchant_transaction_df = spark.sql("""SELECT merchant_name,
        Count(*) AS nooftransactions
        FROM   transactions
        GROUP  BY merchant_name
        ORDER  BY nooftransactions DESC """)

    merchant_transaction_df.coalesce(1).write.format("csv").options(header=True).save("data/output/merchant_transactions")

    transactions_status_merchant_df = spark.sql("""SELECT merchant_name,
            status,
            Count(*) AS nooftransactions
        FROM transactions
        GROUP BY merchant_name,
            status""")

    transactions_status_merchant_df.coalesce(1).write.format("csv").options(header=True).save("data/output/transactions_status_merchant")

    highest_spending_cards_df = spark.sql("""SELECT card_id
        FROM   (SELECT card_id,
               Rank() OVER(ORDER BY spendeur DESC) rnk
        FROM   (SELECT card_id,
                       Sum(Replace(transaction_amount, ',', '') * factor) AS
                       spendEUR
                FROM   transactions ct
                       JOIN currency cr
                         ON ( ct.transaction_currency =
                            cr.transaction_currency )
                WHERE  status = 'completed'
                GROUP  BY card_id))
        WHERE  rnk <= 10""")

    highest_spending_cards_df.coalesce(1).write.format("csv").options(header=True).save("data/output/highest_spending_cards")

# entry point for PySpark application
if __name__ == '__main__':
    main()

