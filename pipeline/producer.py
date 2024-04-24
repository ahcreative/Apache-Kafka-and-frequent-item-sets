import json
import re
from time import sleep
from kafka import KafkaProducer

def remove_html_tags(text):
    # Remove HTML tags from text
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def preprocess_record(record):
    # Remove HTML tags from the 'description' field
    if 'description' in record:
        if isinstance(record['description'], str):
            record['description'] = remove_html_tags(record['description'])
        elif isinstance(record['description'], list):
            # Join the list items into a single string
            record['description'] = ' '.join(record['description'])

    # Convert 'price' to float if it exists
    if 'price' in record:
        try:
            # Convert price range to average if it is represented as a range
            if isinstance(record['price'], str) and '-' in record['price']:
                price_range = record['price'].split('-')
                record['price'] = (float(price_range[0]) + float(price_range[1])) / 2
            else:
                record['price'] = float(record['price'])
        except ValueError:
            record['price'] = None  # Unable to convert to float

    return record

def preprocess_json(input_file, producer):
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            # Load the line as JSON
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Send the preprocessed record to Kafka
            producer.send('preprocessed_data', json.dumps(record).encode('utf-8'))
            sleep(0.1)  # Delay to simulate real-time streaming

    print("Preprocessing completed.")

if _name_ == "_main_":
    # Define input file path
    input_file_path = 'Preprocessed_Amazon_Meta.json'

    # Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Call the function to preprocess the data and stream it to Kafka
    preprocess_json(input_file_path, producer)
