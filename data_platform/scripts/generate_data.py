#!/usr/bin/env python3
"""
Data Generator Script
======================

Generates synthetic test data for the data platform
- Customers CSV
- Products CSV
- Orders CSV

Usage:
    python generate_data.py

Author: hruslanhadi
Date: 2026-04-21
"""

import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import os

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 10000
OUTPUT_DIR = "/opt/airflow/data"

fake = Faker()

def generate_customers(num_records=NUM_CUSTOMERS):
    """Generate customer data."""
    print(f"🏢 Generating {num_records} customer records...")
    
    customers = []
    for i in range(num_records):
        customer = {
            'customer_id': f'CUST_{i+1:06d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'country': fake.country(),
            'city': fake.city(),
            'phone': fake.phone_number(),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d'),
        }
        customers.append(customer)
    
    # Write to CSV
    output_file = os.path.join(OUTPUT_DIR, 'customers.csv')
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
    
    print(f"✅ Generated {len(customers)} customers → {output_file}")
    return customers

def generate_products(num_records=NUM_PRODUCTS):
    """Generate product data."""
    print(f"📦 Generating {num_records} product records...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Toys', 'Food', 'Beauty']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'Premium', 'Deluxe', 'Standard']
    
    products = []
    for i in range(num_records):
        category = random.choice(categories)
        product = {
            'product_id': f'PROD_{i+1:06d}',
            'product_name': f"{fake.word()} {fake.word()}".title(),
            'category': category,
            'subcategory': fake.word().title(),
            'brand': random.choice(brands),
            'unit_price': round(random.uniform(10, 1000), 2),
        }
        products.append(product)
    
    output_file = os.path.join(OUTPUT_DIR, 'products.csv')
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    
    print(f"✅ Generated {len(products)} products → {output_file}")
    return products

def generate_orders(customers, products, num_records=NUM_ORDERS):
    """Generate order data."""
    print(f"🛒 Generating {num_records} order records...")
    
    orders = []
    for i in range(num_records):
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 10)
        discount_percent = random.choice([0, 0, 0, 5, 10, 15])  # Most orders no discount
        
        total_amount = float(product['unit_price']) * quantity
        
        order = {
            'order_id': f'ORD_{i+1:08d}',
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'order_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'quantity': quantity,
            'unit_price': product['unit_price'],
            'total_amount': round(total_amount, 2),
            'discount_percent': discount_percent,
        }
        orders.append(order)
    
    output_file = os.path.join(OUTPUT_DIR, 'orders.csv')
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✅ Generated {len(orders)} orders → {output_file}")
    return orders

def main():
    """Main execution."""
    print("=" * 80)
    print("🔧 Data Platform Test Data Generator")
    print("=" * 80)
    
    # Create output directory if not exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"📁 Output directory: {OUTPUT_DIR}\n")
    
    # Generate data
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(customers, products)
    
    print("\n" + "=" * 80)
    print("✅ Data generation completed successfully!")
    print(f"   - Customers: {len(customers)}")
    print(f"   - Products: {len(products)}")
    print(f"   - Orders: {len(orders)}")
    print("=" * 80)

if __name__ == "__main__":
    main()
