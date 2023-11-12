import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    create_kafka_source_ddl = """
        CREATE TABLE trx_msg(
            order_id INT,
            symbol VARCHAR,
            order_side VARCHAR,
            size FLOAT,
            price FLOAT,
            status VARCHAR,
            created_at INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'technical_assessment',
            'properties.bootstrap.servers' = 'kafka:9091',
            'properties.group.id' = 'test_3',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
        """
    t_env.execute_sql(create_kafka_source_ddl)

    result_sink_ddl = """
        CREATE TABLE result_sink (
            order_id INT,
            symbol VARCHAR,
            order_side VARCHAR,
            size FLOAT,
            price FLOAT,
            status VARCHAR,
            created_at INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:/Users/macbookpro/Downloads/result.json',
            'format' = 'json'
        )
    """

    t_env.execute_sql(result_sink_ddl)

    query = """
        INSERT INTO result_sink
        SELECT * FROM trx_msg
    """

    t_env.execute_sql(query)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:/usr/local/Cellar/apache-flink/1.18.0/libexec/opt/flink-sql-connector-kafka-3.0.1-1.18.jar")

    print("start reading data from kafka")
    read_from_kafka()
