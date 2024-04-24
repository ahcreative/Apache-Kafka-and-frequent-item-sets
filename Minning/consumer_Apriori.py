from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

def generate_candidates(itemset, k):
    return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k])

def prune_itemset(itemset, transactions, min_support, freq_items):
    candidate_counts = defaultdict(int)
    for item in itemset:
        for transaction in transactions:
            if item.issubset(transaction):
                candidate_counts[item] += 1

    pruned_itemset = set()
    local_freq_items = set()
    total_transactions = len(transactions)
    for item, count in candidate_counts.items():
        support = count / total_transactions
        if support >= min_support:
            pruned_itemset.add(item)
            local_freq_items.add(item)
            freq_items[item] = support

    return pruned_itemset, local_freq_items

def apriori(transactions, min_support):
    freq_items = {}
    candidate_itemset = set()
    for transaction in transactions:
        for item in transaction:
            candidate_itemset.add(frozenset([item]))

    k = 2
    current_freq_items = set()
    while True:
        current_freq_items, local_freq_items = prune_itemset(candidate_itemset, transactions, min_support, freq_items)
        if len(current_freq_items) == 0:
            break

        freq_items.update(local_freq_items)
        candidate_itemset = generate_candidates(current_freq_items, k)
        k += 1

    return freq_items

def consume_data():
    consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    transactions = []
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        transactions.append(data['categories'])

    frequent_items = apriori(transactions, 0.1)

    # Connect to MongoDB
    mongo_client = MongoClient('mongodb://localhost:27017/')
    db = mongo_client['frequent_itemset']
    collection = db['consumer_apriori']

    # Insert frequent itemsets into MongoDB
    for itemset, support in frequent_items.items():
        itemset_doc = {
            'itemset': list(itemset),
            'support': support
        }
        collection.insert_one(itemset_doc)

    print("Frequent Itemsets (Apriori Algorithm):")
    for itemset, support in frequent_items.items():
        print(f"{itemset}: {support}")

if __name__ == "__main__":
    consume_data()