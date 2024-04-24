from kafka import KafkaConsumer

def consume_data():
    consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    for message in consumer:
        print(message.value.decode('utf-8'))

if _name_ == "_main_":
    consume_data()
