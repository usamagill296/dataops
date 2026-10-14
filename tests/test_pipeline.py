"""
Tests for DataOps Pipeline

"""

import sys
sys.path.append('src')
from pipeline import extract, transform

# ── Extract Tests ─────────────────────────────
def test_extract_returns_data():
    data = extract('data/transactions.csv')
    assert len(data) > 0

def test_extract_has_required_columns():
    data = extract('data/transactions.csv')
    assert 'id' in data[0]
    assert 'amount' in data[0]
    assert 'country' in data[0]

# ── Transform Tests ───────────────────────────
def test_valid_transaction():
    data = [{'id': 'T1', 'amount': '250',
             'currency': 'EUR', 'country': 'DE',
             'merchant': 'Amazon', 'status': 'completed',
             'date': '2026-10-01'}]
    valid, invalid = transform(data)
    assert len(valid) == 1
    assert len(invalid) == 0

def test_invalid_amount():
    data = [{'id': 'T1', 'amount': 'abc',
             'currency': 'EUR', 'country': 'DE',
             'merchant': 'Amazon', 'status': 'completed',
             'date': '2026-10-01'}]
    valid, invalid = transform(data)
    assert len(invalid) == 1

def test_negative_amount():
    data = [{'id': 'T1', 'amount': '-50',
             'currency': 'EUR', 'country': 'DE',
             'merchant': 'Amazon', 'status': 'completed',
             'date': '2026-10-01'}]
    valid, invalid = transform(data)
    assert len(invalid) == 1

def test_suspicious_country():
    data = [{'id': 'T1', 'amount': '100',
             'currency': 'EUR', 'country': 'XX',
             'merchant': 'Test', 'status': 'completed',
             'date': '2026-10-01'}]
    valid, invalid = transform(data)
    assert valid[0]['is_suspicious'] == True

def test_high_value():
    data = [{'id': 'T1', 'amount': '6000',
             'currency': 'EUR', 'country': 'DE',
             'merchant': 'Test', 'status': 'completed',
             'date': '2026-10-01'}]
    valid, invalid = transform(data)
    assert valid[0]['is_high_value'] == True