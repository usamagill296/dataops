"""
ETL Pipeline
============
Runs complete pipeline WITHOUT Kafka
Used for batch processing and testing

Extract  → Read CSV
Transform → Clean and validate
Load     → Save to files + database
"""

import csv
import json
import sqlite3
import os
from datetime import datetime

OUTPUT_DIR = 'data/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── EXTRACT ───────────────────────────────────
def extract(filepath):
    """Read raw data from CSV"""
    print(f"\n📂 EXTRACT: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    transactions = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            transactions.append(row)
    
    print(f"✅ Extracted {len(transactions)} records")
    return transactions

# ── TRANSFORM ─────────────────────────────────
def transform(transactions):
    """Clean and validate all transactions"""
    print(f"\n🔄 TRANSFORM: {len(transactions)} records")
    
    valid = []
    invalid = []
    
    for record in transactions:
        errors = []
        
        # Validate amount
        try:
            amount = float(record['amount'])
            if amount <= 0:
                errors.append("Negative or zero amount")
        except (ValueError, KeyError):
            errors.append(f"Invalid amount: {record.get('amount')}")
            amount = 0
        
        # Validate country
        country = record.get('country', '')
        if len(country) != 2:
            errors.append(f"Invalid country: {country}")
        
        # Build clean record
        clean = {
            'id': record.get('id'),
            'amount': amount,
            'currency': record.get('currency', 'EUR'),
            'country': country.upper(),
            'merchant': record.get('merchant'),
            'status': record.get('status'),
            'date': record.get('date'),
            'is_suspicious': country in ['XX', 'YY'],
            'is_high_value': amount > 5000,
            'is_valid': len(errors) == 0,
            'errors': errors,
            'processed_at': datetime.now().isoformat()
        }
        
        if errors:
            invalid.append(clean)
            print(f"  ❌ {record.get('id')}: {', '.join(errors)}")
        else:
            valid.append(clean)
            print(f"  ✅ {record.get('id')}: €{amount}")
    
    print(f"\n  Valid: {len(valid)} | Invalid: {len(invalid)}")
    return valid, invalid

# ── LOAD ──────────────────────────────────────
def load_to_json(data, filename):
    """Save to JSON file"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved {len(data)} records → {filepath}")

def load_to_database(data, db_path='data/pipeline.db'):
    """Save to SQLite database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            amount REAL,
            currency TEXT,
            country TEXT,
            merchant TEXT,
            status TEXT,
            date TEXT,
            is_suspicious INTEGER,
            is_high_value INTEGER,
            is_valid INTEGER,
            processed_at TEXT
        )
    ''')
    
    for record in data:
        cursor.execute('''
            INSERT OR REPLACE INTO transactions
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            record['id'], record['amount'],
            record['currency'], record['country'],
            record['merchant'], record['status'],
            record['date'], int(record['is_suspicious']),
            int(record['is_high_value']), int(record['is_valid']),
            record['processed_at']
        ))
    
    conn.commit()
    conn.close()
    print(f"🗄️  Saved {len(data)} records → database")

def generate_report(db_path='data/pipeline.db'):
    """SQL report from database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n" + "="*45)
    print("📊 PIPELINE REPORT")
    print("="*45)
    
    # By country
    cursor.execute('''
        SELECT country, COUNT(*) as total, SUM(amount) as total_amount
        FROM transactions WHERE is_valid=1
        GROUP BY country ORDER BY total_amount DESC
    ''')
    
    print("\n🌍 By Country:")
    print(f"{'Country':<10} {'Count':<8} {'Total €':<12}")
    print("-"*30)
    for row in cursor.fetchall():
        print(f"{row[0]:<10} {row[1]:<8} {row[2]:<12.2f}")
    
    # Suspicious
    cursor.execute('SELECT id, amount, country FROM transactions WHERE is_suspicious=1')
    rows = cursor.fetchall()
    if rows:
        print(f"\n🚨 Suspicious ({len(rows)}):")
        for row in rows:
            print(f"  {row[0]}: €{row[1]} from {row[2]}")
    
    conn.close()

# ── MAIN ──────────────────────────────────────
def run_pipeline():
    print("🚀 DataOps Pipeline Starting...")
    print("="*45)
    
    try:
        raw = extract('data/transactions.csv')
        valid, invalid = transform(raw)
        
        print("\n💾 LOAD:")
        load_to_json(valid, 'valid_transactions.json')
        load_to_database(valid)
        
        if invalid:
            load_to_json(invalid, 'invalid_transactions.json')
        
        generate_report()
        
        print("\n✅ Pipeline completed!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()