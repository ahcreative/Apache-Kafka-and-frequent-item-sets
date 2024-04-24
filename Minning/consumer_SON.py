from kafka import KafkaConsumer
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

def generate_candidates(itemset, k):
    return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k])

def hash_buckets(bucket_count, baskets, threshold):
    buckets = [0] * bucket_count
    for basket in baskets:
        for i, j in combinations(basket, 2):
            index = (i + j) % bucket_count
            buckets[index] += 1
    frequent_buckets = set([i for i, v in enumerate(buckets) if v >= threshold])
    return frequent_buckets

def prune_itemset(itemset, transactions, min_support, freq_items, bucket_count, threshold, frequent_buckets):
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

    pair_counts = defaultdict(int)
    for basket in transactions:
        frequent_items = [item for item in basket if item in frequent_buckets]
        for i, j in combinations(frequent_items, 2):
            if (i, j) in itemset or (j, i) in itemset:
                pair_counts[frozenset([i, j])] += 1
    for pair, count in pair_counts.items():
        support = count / total_transactions
        if support >= min_support:
            pruned_itemset.add(pair)
            local_freq_items.add(pair)
            freq_items[pair] = support
    return pruned_itemset, local_freq_items

def pcy(transactions, min_support):
    freq_items = {}
    candidate_itemset = set()
    for transaction in transactions:
        for item in transaction:
            candidate_itemset.add(frozenset([item]))

    bucket_count = 100
    threshold = min_support * len(transactions)
    frequent_buckets = hash_buckets(bucket_count, transactions, threshold)
    k = 2
    current_freq_items = set()
    while True:
        current_freq_items, local_freq_items = prune_itemset(candidate_itemset, transactions, min_support, freq_items, bucket_count, threshold, frequent_buckets)
        if len(current_freq_items) == 0:
            break
        freq_items.update(local_freq_items)
        candidate_itemset = generate_candidates(current_freq_items, k)
        k += 1
    return freq_items

def generate_initial_candidates(transactions, support):
    initial_candidates = set()
    for transaction in transactions:
        for item in transaction:
            initial_candidates.add(frozenset([item]))
    return initial_candidates

def find_frequent_items(candidate_itemset, transactions, min_support):
    freq_items = {}
    total_transactions = len(transactions)
    for item in candidate_itemset:
        support = sum([1 for transaction in transactions if item.issubset(transaction)]) / total_transactions
        if support >= min_support:
            freq_items[item] = support
    return freq_items

def a_priori(transactions, min_support):
    freq_items = {}
    candidate_itemset = generate_initial_candidates(transactions, min_support)
    freq_items.update(find_frequent_items(candidate_itemset, transactions, min_support))
    k = 2
    while len(candidate_itemset) != 0:
        candidate_itemset = generate_candidates(candidate_itemset, k)
        freq_items.update(find_frequent_items(candidate_itemset, transactions, min_support))
        k += 1
    return freq_items

def son(transactions, min_support, chunk_size):
    freq_items = defaultdict(int)
    chunk_count = len(transactions) // chunk_size

    for i in range(chunk_count):
        chunk = transactions[i * chunk_size: (i + 1) * chunk_size]
        candidate_itemset = a_priori(chunk, min_support)
        for item, support in candidate_itemset.items():
            freq_items[item] += support

    # Find global frequent itemsets
    global_freq_items = {}
    for item, support in freq_items.items():
        if support >= min_support:
            global_freq_items[item] = support

    return global_freq_items

def consume_data():
    consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    transactions = []
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['frequent_itemset']
    collection = db['consumer_son']

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        transactions.append(data['categories'])

    frequent_items = son(transactions, 0.1, 500)

    print("\nFrequent Itemsets (SON Algorithm):")

    # Store frequent itemsets in MongoDB
    collection.delete_many({})  # Clear the collection before inserting new data
    for itemset, support in frequent_items.items():
        print(f"{itemset}: {support}")
        collection.insert_one({'itemset': list(itemset), 'support': support})

if __name__ == "__main__":
    consume_data()
