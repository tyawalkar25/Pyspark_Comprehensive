import json
import random
from datetime import datetime,timedelta

records = []
product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys & Games']
countries = ["India", "USA", "UK", "CANADA", "UAE","GERMANY", "FRANCE"]
payment_methods = ['Credit Card', 'Debit Card', 'Net Banking', 'UPI', 'Cash on Delivery']

for i in range(100000):
    date = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')

    # Create nested JSON structure
    record = {
        'transaction_id': f'TRX-{i + 1:06d}',
        'date': date,
        'product': {
            'id': f'PROD-{random.randint(1000, 9999)}',
            'name': f'Product-{random.randint(1, 1000)}',
            'category': random.choice(product_categories),
            'specifications': {
                'weight': round(random.uniform(0.1, 10.0), 2),
                'dimensions': {
                    'length': round(random.uniform(5, 50), 2),
                    'width': round(random.uniform(5, 50), 2),
                    'height': round(random.uniform(5, 50), 2)
                }
            }
        },
        'customer': {
            'id': f'CUST-{random.randint(1000, 9999)}',
            'location': {
                'country': random.choice(countries),
                'city': f'City-{random.randint(1, 100)}'
            }
        },
        'payment': {
            'method': random.choice(payment_methods),
            'amount': round(random.uniform(10, 1000), 2),
            'currency': 'USD',
            'status': random.choice(['completed', 'pending', 'failed']),
            'details': {
                'transaction_fee': round(random.uniform(1, 10), 2),
                'tax': round(random.uniform(5, 50), 2)
            }
        },
        'shipping': {
            'method': random.choice(['Standard', 'Express', 'Priority']),
            'cost': round(random.uniform(5, 50), 2),
            'estimated_days': random.randint(1, 10)
        }
    }
    records.append(record)

with open('sales_data.json', 'w') as f:
    json.dump(records,f)

print("JSON Dataset created successfully")

