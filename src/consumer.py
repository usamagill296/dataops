"""
Kafka Consumer
==============
Reads transactions from Kafka topic
Transforms (cleans + validates) each one
Saves results to JSON files


"""

from kafka import KafkaConsumer
import json
import os
from datetime import datetime

# Where to save processed data
OUTPUT_DIR = 'data/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_consumer(topic):
    """Create Kafka consumer"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # read from beginning
        value_deserializer=lambda data: json.loads(data.decode('utf-8')),
        group_id='dataops-consumer-group',
        consumer_timeout_ms=10000  # stop after 10 seconds of no messages
    )

def validate_transaction(transaction):
    """
    Check if transaction is valid
    Returns (is_valid, errors, clean_data)
    """
    errors = []
    
    # Check amount
    try:
        amount = float(transaction['amount'])
        if amount <= 0:
            errors.append(f"Invalid amount: {transaction['amount']}")
    except (ValueError, KeyError):
        errors.append(f"Amount not a number: {transaction.get('amount')}")
        amount = 0
    
    # Check country
    country = transaction.get('country', '')
    if len(country) != 2:
        errors.append(f"Invalid country: {country}")
    
    # Check status
    valid_statuses = ['completed', 'pending', 'failed']
    status = transaction.get('status', '')
    if status not in valid_statuses:
        errors.append(f"Invalid status: {status}")
    
    # Build clean record
    clean = {
        'id': transaction.get('id', 'UNKNOWN'),
        'amount': amount,
        'currency': transaction.get('currency', 'EUR'),
        'country': country.upper(),
        'merchant': transaction.get('merchant', ''),
        'status': status,
        'date': transaction.get('date', ''),
        'is_suspicious': country in ['XX', 'YY'],
        'is_high_value': amount > 5000,
        'is_valid': len(errors) == 0,
        'errors': errors,
        'processed_at': datetime.now().isoformat()
    }
    
    return len(errors) == 0, errors, clean

def save_to_json(data, filename):
    """Save list of records to JSON file"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved {len(data)} records to {filepath}")

def consume_and_process(topic):
    """Main consumer loop"""
    print(f"👂 Listening to '{topic}' topic...")
    
    consumer = create_consumer(topic)
    
    valid_records = []
    invalid_records = []
    
    for message in consumer:
        transaction = message.value
        
        # Validate and clean
        is_valid, errors, clean = validate_transaction(transaction)
        
        if is_valid:
            valid_records.append(clean)
            flag = "🚨 SUSPICIOUS" if clean['is_suspicious'] else "✅"
            flag += " 💰 HIGH VALUE" if clean['is_high_value'] else ""
            print(f"{flag} {clean['id']}: €{clean['amount']} from {clean['country']}")
        else:
            invalid_records.append(clean)
            print(f"❌ {clean['id']}: {', '.join(errors)}")
    
    # Save results
    print(f"\n📊 Processing complete!")
    print(f"   Valid:   {len(valid_records)}")
    print(f"   Invalid: {len(invalid_records)}")
    
    if valid_records:
        save_to_json(valid_records, 'valid_transactions.json')
    
    if invalid_records:
        save_to_json(invalid_records, 'invalid_transactions.json')
    
    return valid_records, invalid_records

if __name__ == "__main__":
    consume_and_process('transactions')