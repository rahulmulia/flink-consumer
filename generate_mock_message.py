# import time
# from kafka import KafkaProducer
# import pandas as pd


# def send_data_from_csv(topic=None, csv_path=None):
#     # connect to kafka
#     producer = KafkaProducer(bootstrap_servers='localhost:9092')
#     dataframe = pd.read_csv(csv_path)

#     # loop each row in dataframe, dump to json and send to kafka
#     for i in dataframe.index:
#         data = dataframe.loc[i].to_json(orient='index', indent=1)
#         producer.send(topic, data.encode('utf-8'))

#         print(data)

#         if i % 10 == 0:
#             time.sleep(0.1)

#     # block until all async messages are sent
#     producer.flush()


# if __name__ == '__main__':
#     send_data_from_csv("technical_assessment", "./order_book_mockup.csv")

# exploration
import time
from confluent_kafka import Producer
import pandas as pd
import json
import numpy as np

def convert_to_serializable(value):
    if isinstance(value, np.int64):
        return int(value)
    return value

def send_data_from_csv(topic=None, csv_path=None, bootstrap_servers='localhost:9091'):
    # Set up Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': bootstrap_servers
    }

    # Create Kafka producer instance
    producer = Producer(producer_conf)
    
    dataframe = pd.read_csv(csv_path)

    # loop through each row in the dataframe, dump to json and send to kafka
    for i in dataframe.index:
        # data = dataframe.loc[i].to_dict()
        data = dataframe.loc[i].apply(convert_to_serializable).to_dict()
        json_data = json.dumps(data)
        
        # Produce the message to Kafka
        producer.produce(topic, value=json_data)

        print(json_data)

        if i % 10 == 0:
            producer.flush()  # Flush messages every 10 iterations
            time.sleep(0.1)

    # Flush remaining messages
    producer.flush()

if __name__ == '__main__':
    send_data_from_csv("technical_assessment", "./order_book_mockup_copy.csv")

