# Apache-Kafka-and-frequent-item-sets
# Introduction
This GitHub repository contains a comprehensive preprocessing and mining pipeline for efficiently handling large datasets, particularly focusing on JSON data. The pipeline includes preprocessing steps to clean and prepare the data, followed by mining algorithms to extract valuable insights.

# Approach
The approach combines modularization and scalability to handle large volumes of data effectively. Each component of the pipeline is designed to be modular and easily extensible, allowing users to customize and scale the pipeline according to their specific needs.

# Components
# Preprocessing and Sampling
The `preprocessing_and_sampling.py` script focuses on extracting and preprocessing data from JSON files.

It includes functions to extract a specified amount of data and preprocess it by removing HTML tags, converting price formats, and eliminating duplicates.
# Mining Algorithms
Three mining algorithms are implemented:

- Apriori Algorithm (`consumer_apriori.py`): Finds frequent itemsets from streaming data using the Apriori algorithm. Utilizes Kafka for data streaming and MongoDB for storage.

- PCY Algorithm (`consumer_pcy.py`): Implements the PCY (Park-Chen-Yu) algorithm for finding frequent itemsets from streaming data. Similar to Apriori, it uses Kafka and MongoDB.

- SON Algorithm (`consumer_son.py`): Utilizes the SON (Savasere, Omiecinski, and Navathe) algorithm to find frequent itemsets from streaming data. Also employs Kafka and MongoDB.

# Pipeline Consumers
Three consumer scripts (`consumer_1.py`, `consumer_2.py`, `consumer_3.py`) represent consumers in the data processing pipeline.

They consume preprocessed data from Kafka for further processing or analysis.
# Pipeline Producer
The `producer.py` script acts as a producer in the data processing pipeline.
It preprocesses data from a JSON file and streams it to Kafka for consumption by consumers.
# Usage
- Ensure `Kafka` and `MongoDB` are set up and running.

- Execute pipeline/`producer.py` to preprocess data and stream it to Kafka.

- Run any of the mining scripts (`consumer_apriori.py`, `consumer_pcy.py`, `consumer_son.py`) to analyze frequent itemsets from the streamed data.

- Optionally, run the consumer scripts (`consumer_1.py`, `consumer_2.py`, `consumer_3.py`) for further processing in the pipeline.
# Dependencies
- `Python3`

- `Kafka`

- `MongoDB`
