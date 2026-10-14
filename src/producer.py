"""
Kafka Producer
==============
Reads transactions from CSV
Sends each one to Kafka topic


"""

from kafka import KafkaProducer
import json
import csv
import time
import os

def create_producer():
    """
    Create Kafka producer
    bootstrap_servers = address of Kafka
    value_serializer = convert dict to bytes
    """
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

def read_csv(filepath):
    """Read transactions from CSV file"""
    transactions = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            transactions.append(row)
    return transactions

def send_to_kafka(producer, topic, transactions):
    """Send each transaction to Kafka topic"""
    print(f"🚀 Sending {len(transactions)} transactions to '{topic}' topic...")
    
    for transaction in transactions:
        # Send to Kafka
        producer.send(topic, value=transaction)
        print(f"✅ Sent: {transaction['id']} - €{transaction['amount']} from {transaction['country']}")
        time.sleep(0.5)  # small delay between messages
    
    # Make sure all messages delivered
    producer.flush()
    print(f"\n✅ All {len(transactions)} transactions sent to Kafka!")

if __name__ == "__main__":
    # Create producer
    producer = create_producer()
    
    # Read transactions
    transactions = read_csv('data/transactions.csv')
    
    # Send to Kafka
    send_to_kafka(producer, 'transactions', transactions)   