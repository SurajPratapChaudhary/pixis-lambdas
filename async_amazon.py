import os
os.environ['ENV_DISABLE_DONATION_MSG'] = '1'
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, Float, MetaData, insert, update, select, Text, case, inspect, VARCHAR, DECIMAL, DATETIME
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import bindparam
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
import numpy as np
from sp_api.api import ListingsItems, Reports, Feeds
from sp_api.base import Marketplaces, SellingApiException, ReportType
import time
import csv
from io import StringIO
import requests 
import json 
import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import random
import sys
from datetime import datetime, timedelta
import gzip 
import io
import chardet 
from functools import wraps 
import re
import hashlib
from typing import Any, List
import random
import fcntl
import atexit

global_amazon_skus = set()


# Load environment variables (for SSH connection)
load_dotenv()

# SSH configuration
SSH_HOST = os.getenv('SSH_IP_HOST')
SSH_USER = os.getenv('SSH_SQL_USER')
SSH_PASSWORD = os.getenv('SSH_SQL_PASSWORD')

# Database configuration (using the constants you provided)
DB_USER = 'ilanrbental'
DB_PASSWORD = 'Shurerider%40123'
DB_HOST = 'localhost'
DB_NAME = 'P1MotoProductData'
DB_PORT = 3306

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# If you want to reduce SQL Alchemy's output
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
LOCK_FILE = "/tmp/amazon_data_processor.lock"

def create_lock_table(connection):
    metadata = MetaData()
    lock_table = Table(
        'distributed_lock', metadata,
        Column('lock_name', String(255), primary_key=True),
        Column('locked_at', DateTime),
        Column('locked_until', DateTime)
    )
    metadata.create_all(connection)
    return lock_table

def acquire_lock(connection, lock_name="amazon_data_processor_lock", timeout_minutes=60):
    lock_table = create_lock_table(connection)
    now = datetime.utcnow()
    lock_until = now + timedelta(minutes=timeout_minutes)
    
    try:
        # Try to insert a new lock
        insert_stmt = lock_table.insert().values(
            lock_name=lock_name,
            locked_at=now,
            locked_until=lock_until
        )
        connection.execute(insert_stmt)
        connection.commit()
        return True
    except SQLAlchemyError:
        # If insert fails, try to update an expired lock
        update_stmt = lock_table.update().where(
            (lock_table.c.lock_name == lock_name) &
            (lock_table.c.locked_until < now)
        ).values(
            locked_at=now,
            locked_until=lock_until
        )
        result = connection.execute(update_stmt)
        connection.commit()
        return result.rowcount > 0


def release_lock(connection, lock_name="amazon_data_processor_lock"):
    lock_table = create_lock_table(connection)
    delete_stmt = lock_table.delete().where(lock_table.c.lock_name == lock_name)
    connection.execute(delete_stmt)
    connection.commit()

def extend_lock(connection, lock_name="amazon_data_processor_lock", extend_minutes=60):
    lock_table = create_lock_table(connection)
    now = datetime.utcnow()
    new_lock_until = now + timedelta(minutes=extend_minutes)
    
    update_stmt = lock_table.update().where(
        (lock_table.c.lock_name == lock_name) &
        (lock_table.c.locked_until > now)
    ).values(
        locked_until=new_lock_until
    )
    result = connection.execute(update_stmt)
    connection.commit()
    return result.rowcount > 0

async def periodic_lock_renewal(connection, lock_name="amazon_data_processor_lock", interval_minutes=5):
    while True:
        await asyncio.sleep(interval_minutes * 60)
        if not extend_lock(connection, lock_name):
            logging.error("Failed to renew lock. Terminating script.")
            # Optionally, you can raise an exception here to force the script to stop
            raise Exception("Lock renewal failed")

def check_lock_status(connection, lock_name="amazon_data_processor_lock"):
    lock_table = create_lock_table(connection)
    now = datetime.utcnow()
    
    select_stmt = lock_table.select().where(
        (lock_table.c.lock_name == lock_name) &
        (lock_table.c.locked_until > now)
    )
    result = connection.execute(select_stmt)
    return result.fetchone() is not None

#--------------------------------------------------new stuff

def comprehensive_amazon_data_process(connection, user_id, selling_partner_id, df_products, skus_to_delete=None):
    metadata = MetaData()

    # Define tables
    batch_history_table = Table(
        f'batch_history_{user_id}', metadata,
        Column('batch_id', String(255), primary_key=True),
        Column('seller_id', String(255)),
        Column('total_messages', Integer),
        Column('status', String(255)),
        Column('failed_message', String(255)),
        Column('successful_messages', Integer),
        Column('started_at', DateTime),
        Column('ended_at', DateTime)
    )

    change_history_table = Table(
        f'change_history_{user_id}', metadata,
        Column('P1Moto_SKU', String(255), primary_key=True),
        Column('batch_id', String(255)),
        Column('seller_id', String(255)),
        Column('asin', String(255)),
        Column('title', String(255)),
        Column('price', Float),
        Column('status', String(255)),
        Column('error_description', Text),
        Column('started_at', DateTime),
        Column('ended_at', DateTime),
        Column('product_hash', String(64))
    )

    # Create tables if they don't exist
    metadata.create_all(connection)

    # Start batch process
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    start_time = datetime.now()

    # Prepare batch history data
    batch_data = {
        'batch_id': batch_id,
        'seller_id': selling_partner_id,
        'status': 'processing',
        'started_at': start_time
    }

    # Bulk upsert for batch history
    stmt = insert(batch_history_table).values(batch_data)
    upsert_batch_stmt = stmt.on_duplicate_key_update(
        seller_id=stmt.inserted.seller_id,
        status=stmt.inserted.status,
        started_at=stmt.inserted.started_at
    )
    connection.execute(upsert_batch_stmt)

    logging.info(f"Starting comprehensive_amazon_data_process for user {user_id}")

    # Prepare data for change history upsert
    change_data = []
    for _, row in df_products.iterrows():
        try:
            price = round(float(row['price_to_push']), 2) if pd.notna(row['price_to_push']) and row['price_to_push'] != '' and row['price_to_push'] != 'nan' else None
        except ValueError:
            price = None

        change_data.append({
            'P1Moto_SKU': str(row['P1Moto SKU']),
            'batch_id': str(batch_id),
            'seller_id': str(selling_partner_id),
            'asin': str(row['ASIN']) if pd.notna(row['ASIN']) else None,
            'title': str(row['Title']) if pd.notna(row['Title']) else None,
            'price': price,
            'status': 'processing',
            'started_at': start_time,
            'ended_at': None,
            'error_description': None,
            'product_hash': row.get('product_hash')  # Use .get() to handle cases where product_hash might not exist
        })

    # Add SKUs to delete
    if skus_to_delete:
        for sku in skus_to_delete:
            change_data.append({
                'P1Moto_SKU': str(sku),
                'batch_id': str(batch_id),
                'seller_id': str(selling_partner_id),
                'asin': None,
                'title': None,
                'price': None,
                'status': 'pending_delete',
                'started_at': start_time,
                'ended_at': None,
                'error_description': None,
                'product_hash': None  # Set product_hash to None for SKUs to delete
            })


    try:
        # Bulk upsert for batch history
        stmt = insert(batch_history_table).values(batch_data)
        upsert_batch_stmt = stmt.on_duplicate_key_update(
            seller_id=stmt.inserted.seller_id,
            status=stmt.inserted.status,
            started_at=stmt.inserted.started_at
        )
        connection.execute(upsert_batch_stmt)

        # Perform bulk upsert for change history in batches
        chunk_size = 1000  # Adjust based on your needs
        for i in range(0, len(change_data), chunk_size):
            chunk = change_data[i:i+chunk_size]
            stmt = insert(change_history_table)
            upsert_change_stmt = stmt.on_duplicate_key_update(
                batch_id=stmt.inserted.batch_id,
                seller_id=stmt.inserted.seller_id,
                asin=stmt.inserted.asin,
                title=stmt.inserted.title,
                price=stmt.inserted.price,
                status=stmt.inserted.status,
                started_at=stmt.inserted.started_at,
                ended_at=stmt.inserted.ended_at,
                error_description=stmt.inserted.error_description,
                product_hash=case(
                    (stmt.inserted.product_hash != None, stmt.inserted.product_hash),
                    else_=change_history_table.c.product_hash
                )
            )
            connection.execute(upsert_change_stmt, chunk)
            logging.info(f"Inserted/updated {len(chunk)} records for user {user_id}")

        # Commit the transaction
        connection.commit()
        logging.info(f"Successfully processed data for user {user_id}")
    except Exception as e:
        connection.rollback()
        logging.error(f"Database error in comprehensive_amazon_data_process for user {user_id}: {str(e)}")
        raise

    MESSAGE_ID_START = 1  # Start from 1 to avoid any potential issues with 0
    sku_to_message_id = {item['P1Moto_SKU']: i + MESSAGE_ID_START for i, item in enumerate(change_data)}
    
    logging.info(f"Completed comprehensive_amazon_data_process for user {user_id}")
    return batch_id, sku_to_message_id

def check_and_recreate_change_history_table(connection, user_id):
    metadata = MetaData()
    inspector = inspect(connection)
    table_name = f'change_history_{user_id}'
    
    expected_columns = {
        'P1Moto_SKU': String,
        'batch_id': String,
        'seller_id': String,
        'asin': String,
        'title': String,
        'price': Float,
        'status': String,
        'error_description': Text,
        'started_at': DateTime,
        'ended_at': DateTime,
        'product_hash': String
    }
    
    if inspector.has_table(table_name):
        existing_columns = {col['name']: col['type'] for col in inspector.get_columns(table_name)}
        mismatch = False
        for col_name, expected_type in expected_columns.items():
            if col_name not in existing_columns:
                mismatch = True
                logging.info(f"Column {col_name} is missing in {table_name}")
                break
            existing_type = existing_columns[col_name]
            if not isinstance(existing_type, expected_type):
                # Check for compatible types
                if (isinstance(expected_type, String) and isinstance(existing_type, (String, VARCHAR, Text))) or \
                   (expected_type == Float and isinstance(existing_type, (Float, DECIMAL))) or \
                   (expected_type == DateTime and isinstance(existing_type, DATETIME)):
                    continue
                mismatch = True
                logging.info(f"Column {col_name} has unexpected type in {table_name}. Expected {expected_type}, got {existing_type}")
                break
        
        if mismatch:
            logging.info(f"Schema mismatch for {table_name}. Dropping and recreating the table.")
            connection.execute(text(f"DROP TABLE {table_name}"))
        else:
            logging.info(f"Schema for {table_name} is correct.")
            return
    
    # Create the table with the correct schema
    change_history_table = Table(
        table_name, metadata,
        Column('P1Moto_SKU', String(255), primary_key=True),
        Column('batch_id', String(255)),
        Column('seller_id', String(255)),
        Column('asin', String(255)),
        Column('title', String(255)),
        Column('price', Float),
        Column('status', String(255)),
        Column('error_description', Text),
        Column('started_at', DateTime),
        Column('ended_at', DateTime),
        Column('product_hash', String(64))
    )
    metadata.create_all(connection, tables=[change_history_table])
    logging.info(f"Created {table_name} with the correct schema.")

def create_product_hash(df: pd.DataFrame) -> pd.DataFrame:
    def hash_row(row: pd.Series) -> str:
        # Convert all values to strings and join them
        values: List[Any] = row.values.tolist()
        string_values: List[str] = [str(value) if pd.notna(value) else '' for value in values]
        row_string: str = '|'.join(string_values)
        
        # Create SHA256 hash
        return hashlib.sha256(row_string.encode('utf-8')).hexdigest()
    #print all columns
    print(f"Columns used to make hash: {df.columns}")
    # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()
    
    # Apply the hash function to each row
    df_copy['product_hash'] = df_copy.apply(hash_row, axis=1)
    
    return df_copy

def filter_unchanged_products(connection, user_id, df_products):
    change_history_table = Table(f'change_history_{user_id}', MetaData(), autoload_with=connection)
    
    # Query to get the latest status, hash, and ended_at for each SKU
    query = select(
        change_history_table.c.P1Moto_SKU,
        change_history_table.c.status,
        change_history_table.c.product_hash,
        change_history_table.c.ended_at
    ).where(
        change_history_table.c.P1Moto_SKU.in_(df_products['P1Moto SKU'].tolist())
    )
    
    result = connection.execute(query)
    
    # Convert result to a dictionary for easy lookup
    history_data = {row.P1Moto_SKU: row for row in result}
    
    current_time = datetime.now()
    keep_probability = 0.03  # 10% chance to keep unchanged products

    def should_keep_product(row):
        sku = row['P1Moto SKU']
        if sku not in history_data:
            return True  # Keep new products
        
        history = history_data[sku]
        
        if history.status != 'processed':
            return True  # Keep products that weren't successfully processed last time
        
        if history.product_hash != row['product_hash']:
            return True  # Keep products with changed hash

        if history.ended_at is None or (current_time - history.ended_at).days >= 10:
            return True  # Keep products that haven't been updated in 10 days or more

        # For products that passed all above conditions, keep with 10% probability
        return random.random() < keep_probability

    filtered_df = df_products[df_products.apply(should_keep_product, axis=1)]
    
    total_products = len(df_products)
    filtered_products = len(filtered_df)
    changed_products = sum(
        row['product_hash'] != history_data[row['P1Moto SKU']].product_hash 
        if row['P1Moto SKU'] in history_data else True 
        for _, row in filtered_df.iterrows()
    )
    unchanged_kept = filtered_products - changed_products

    logging.info(f"Total products before filtering: {total_products} for user {user_id}")
    logging.info(f"Filtered out {total_products - filtered_products} unchanged products for user {user_id}")
    logging.info(f"Kept {changed_products} changed products for user {user_id}")
    logging.info(f"Kept {unchanged_kept} unchanged products due to 3% random selection for user {user_id}")

    return filtered_df

def ensure_product_hash_column(connection, user_id):
    metadata = MetaData()
    
    # Define the table structure
    change_history_table = Table(
        f'change_history_{user_id}', metadata,
        Column('P1Moto_SKU', String(255), primary_key=True),
        Column('batch_id', String(255)),
        Column('seller_id', String(255)),
        Column('asin', String(255)),
        Column('title', String(255)),
        Column('price', Float),
        Column('status', String(255)),
        Column('error_description', Text),
        Column('started_at', DateTime),
        Column('ended_at', DateTime),
        Column('product_hash', String(64))
    )

    # Create the table if it doesn't exist
    if not connection.dialect.has_table(connection, f'change_history_{user_id}'):
        logging.info(f"Creating change_history_{user_id} table")
        metadata.create_all(connection, tables=[change_history_table])
        logging.info(f"change_history_{user_id} table created")
    else:
        # If the table exists, check if the product_hash column exists
        inspector = inspect(connection)
        columns = inspector.get_columns(f'change_history_{user_id}')
        column_names = [col['name'] for col in columns]
        
        if 'product_hash' not in column_names:
            logging.info(f"Adding product_hash column to change_history_{user_id} table")
            
            # Check if we're already in a transaction
            in_transaction = connection.in_transaction()
            
            try:
                if not in_transaction:
                    connection.execute(text("START TRANSACTION"))
                
                connection.execute(text(f"""
                    ALTER TABLE change_history_{user_id}
                    ADD COLUMN product_hash VARCHAR(64)
                """))
                
                if not in_transaction:
                    connection.execute(text("COMMIT"))
                
                logging.info(f"product_hash column added to change_history_{user_id} table")
            except Exception as e:
                if not in_transaction:
                    connection.execute(text("ROLLBACK"))
                logging.error(f"Error adding product_hash column: {str(e)}")
                raise
        else:
            logging.info(f"product_hash column already exists in change_history_{user_id} table")

def sanitize_string(value, max_length=255):
    if value is None:
        return None
    value = str(value)
    # Remove any non-printable characters
    value = ''.join(char for char in value if char.isprintable())
    # Truncate to max_length
    return value[:max_length]

def sanitize_float(value):
    if value is None or value == '' or value == 'nan':
        return None
    try:
        return float(value)
    except ValueError:
        return None

def sanitize_int(value):
    if value is None or value == '' or value == 'nan':
        return None
    try:
        return int(value)
    except ValueError:
        return None

def sanitize_datetime(value):
    if isinstance(value, datetime):
        return value
    return None

def update_amazon_process_results(connection, user_id, batch_id, sku_to_message_id, amazon_results):
    change_history_table = Table(f'change_history_{user_id}', MetaData(), autoload_with=connection)
    batch_history_table = Table(f'batch_history_{user_id}', MetaData(), autoload_with=connection)

    end_time = datetime.now()
    total_processed = 0
    total_successful = 0
    total_failed = 0

    # Update all records in change_history table to mark them as processed
    update_all_stmt = change_history_table.update().where(
        change_history_table.c.batch_id == batch_id
    ).values(
        ended_at=end_time,
        status='processed'
    )
    connection.execute(update_all_stmt)
    logging.info(f"Marked all records as processed for batch {batch_id}")

    # Process Amazon results
    update_data = []
    for result in amazon_results:
        if isinstance(result, dict) and 'error' not in result:
            summary = result.get('summary', {})
            total_processed += summary.get('messagesProcessed', 0)
            total_successful += summary.get('messagesSuccessful', 0)
            total_failed += summary.get('messagesFailed', 0)

            for item in result.get('results', []):
                sku = item.get('sku')
                if sku:
                    status = 'success' if item['status'] == 'SUCCESS' else 'failed'
                    error_description = item.get('errorMessage', '')
                    if len(error_description) > 65535:  # MySQL Text limit
                        error_description = error_description[:65532] + '...'
                    
                    update_data.append({
                        'b_P1Moto_SKU': sanitize_string(sku, 255),
                        'status': sanitize_string(status, 255),
                        'error_description': error_description,
                        'ended_at': sanitize_datetime(end_time)
                    })

    # Perform batch update for change history
    if update_data:
        chunk_size = 1000  # Adjust based on your needs
        for i in range(0, len(update_data), chunk_size):
            chunk = update_data[i:i+chunk_size]
            try:
                result = connection.execute(
                    change_history_table.update().where(
                        change_history_table.c.P1Moto_SKU == bindparam('b_P1Moto_SKU')
                    ),
                    chunk
                )
                logging.info(f"Updated {result.rowcount} records in change history for user {user_id}")
            except Exception as e:
                logging.error(f"Error updating change history: {str(e)}")

    # Update batch history
    status = 'completed' if total_failed == 0 else 'completed_with_errors'
    try:
        result = connection.execute(
            batch_history_table.update().
            where(batch_history_table.c.batch_id == batch_id).
            values(
                total_messages=sanitize_int(total_processed),
                successful_messages=sanitize_int(total_successful),
                failed_message=sanitize_string(str(total_failed), 255),
                status=sanitize_string(status, 255),
                ended_at=sanitize_datetime(end_time)
            )
        )
        logging.info(f"Updated batch history for user {user_id}. Rows affected: {result.rowcount}")
    except Exception as e:
        logging.error(f"Error updating batch history: {str(e)}")

    connection.commit()

    return {
        'total_messages': total_processed,
        'successful_messages': total_successful,
        'failed_messages': total_failed,
        'status': status
    }
#----------------------------------------------------end new stuff


def get_db_connection():
    """
    Establish an SSH tunnel and return a SQLAlchemy engine.
    """
    try:
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, 22),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(DB_HOST, DB_PORT)
        )
        
        tunnel.start()
        
        local_port = tunnel.local_bind_port
        logging.info(f"Attempting to connect to database at 127.0.0.1:{local_port}")
        engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{local_port}/{DB_NAME}', future=True)


        
        return tunnel, engine
    except Exception as e:
        logging.error(f"Error establishing SSH tunnel: {str(e)}")
        raise

def get_all_user_ids(connection, specific_user_id=None):
    try:
        query = """
            SELECT cu.id, o.access_token, o.refresh_token, c.selling_partner_id
            FROM custom_user cu
            LEFT JOIN oauth o ON cu.id = o.user_id
            LEFT JOIN channels c ON cu.id = c.user_id
            WHERE cu.is_superuser = 0
        """
        
        if specific_user_id:
            query += f" AND cu.id = {specific_user_id}"
        
        result = connection.execute(text(query))
        
        users = []
        for row in result:
            if row[1] and row[2] and row[3]:  # Ensure we have all necessary data
                users.append({
                    'id': row[0],
                    'access_token': row[1],
                    'refresh_token': row[2],
                    'selling_partner_id': row[3]
                })
            else:
                logging.warning(f"Incomplete data for user_id: {row[0]}")

        return users
    except Exception as e:
        logging.error(f"Error retrieving user IDs and tokens: {str(e)}")
        raise

def get_dataframe(connection, table_name):
    """
    Retrieve data from a given table and return it as a DataFrame with all columns as strings.
    """
    query = text(f"SELECT * FROM {table_name}")
    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df.astype(str)  # Convert all columns to string type

def get_asins_data(connection, skus):
    """
    Retrieve ASIN, UPC, EAN, SellerFulfilledReferralFee, and MPN_Match from P1ASINS table for given SKUs.
    """
    if not skus:
        return pd.DataFrame()  # Return an empty DataFrame if no SKUs are provided
    
    sku_list = ", ".join([f"'{sku}'" for sku in skus])
    query = text(f"""
        SELECT `P1Moto SKU`, ASIN, UPC, EAN, SellerFulfilledReferralFee, MPN_Match, asin_item_name, product_type, number_of_items
        FROM P1ASINS 
        WHERE `P1Moto SKU` IN ({sku_list})
    """)
    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df.astype(str)  # Convert all columns to string type

def get_p1moto_data(connection, skus):
    """
    Retrieve additional data from P1Moto table for given SKUs.
    """
    if not skus:
        return pd.DataFrame()
    
    sku_list = ", ".join([f"'{sku}'" for sku in skus])
    query = text(f"""
        SELECT * 
        FROM P1Moto 
        WHERE `P1Moto SKU` IN ({sku_list})
    """)
    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df.astype(str)  # Convert all columns to string type

def calculate_price_to_push(fulfillment_cost, referral_fee, request_margin):
    try:
        fulfillment_cost = float(fulfillment_cost)
        referral_fee = float(referral_fee)
        request_margin = float(request_margin)
        
        if fulfillment_cost <= 0 or (1 - referral_fee - request_margin) <= 0:
            return np.nan
        
        price_to_push = fulfillment_cost / (1 - referral_fee - request_margin)
        return round(price_to_push, 4)
    except ValueError:
        return np.nan


def process_user_data(connection, user_id):
    """
    Process data for a given user ID.
    """
    try:
        # Read data into DataFrames
        df_products = get_dataframe(connection, f"seller_products_{user_id}")
        
        if df_products.empty:
            logging.warning(f"No data found in seller_products_{user_id}")
            return None

        # Get ASIN data
        skus = df_products['P1Moto SKU'].tolist()
        asins_data = get_asins_data(connection, skus)
        
        # Merge ASIN data with products data
        df_products = pd.merge(df_products, asins_data, on='P1Moto SKU', how='left')
        
        # Apply MPN_Match logic (compare strings)
        df_products.loc[df_products['MPN_Match'].str.lower() == 'false', 'Push to Amazon'] = 'No'
        
        # Read overwrite data
        df_overwrite = get_dataframe(connection, f"seller_products_overwrite_{user_id}")

        # Apply overwrite logic
        for index, row in df_overwrite.iterrows():
            sku = row['P1Moto SKU']
            if sku in df_products['P1Moto SKU'].values:
                for column in df_overwrite.columns:
                    if column in df_products.columns:
                        value = row[column]
                        if pd.notna(value) and value != '' and value is not None and value != 'nan' and value != 'NULL' and value != 'None':
                            df_products.loc[df_products['P1Moto SKU'] == sku, column] = str(value)
        #---------------------------------------------Brand Rules Logic
        # Check if brand_rules table exists for the user
        brand_rules_table = f"brand_rules_{user_id}"
        if connection.dialect.has_table(connection, brand_rules_table):
            # Read brand rules into DataFrame
            df_brand_rules = get_dataframe(connection, brand_rules_table)
            print(f"Retrieved brand rules for user {user_id}")
            
            # Drop 'id' column and rename others
            df_brand_rules = df_brand_rules.drop(columns=['id'])
            df_brand_rules = df_brand_rules.rename(columns={
                'brand_name': 'brand_rules_brand_name',
                'MAP Price': 'brand_rules_map_percentage',
                'Push to Amazon': 'brand_rules_push_to_amazon',
                'Push to Website': 'brand_rules_push_to_website'
            })
            
            # Function to normalize brand names
            def normalize_brand(brand):
                return brand.lower().strip() if isinstance(brand, str) else brand
            
            # Create normalized columns for comparison
            df_brand_rules['normalized_brand'] = df_brand_rules['brand_rules_brand_name'].apply(normalize_brand)
            df_products['normalized_brand'] = df_products['Brand'].apply(normalize_brand)
            
            # Merge brand rules with products using normalized brand names
            df_products = pd.merge(df_products, df_brand_rules, left_on='normalized_brand', right_on='normalized_brand', how='left')
            
            # Apply brand rules only if overwrite 'Push to Amazon' is not 'No'
            mask = df_products['brand_rules_brand_name'].notna()
            mask_overwrite_not_no = df_products['Push to Amazon'] != 'No'  # Ensure overwrite 'No' is not overridden

            # Update Push to Amazon only if brand rule value is valid and overwrite is not 'No'
            mask_amazon = (
                mask &
                mask_overwrite_not_no &
                df_products['brand_rules_push_to_amazon'].notna() &
                (df_products['brand_rules_push_to_amazon'] != '') &
                (df_products['brand_rules_push_to_amazon'] != 'nan')
            )

            df_products.loc[mask_amazon, 'Push to Amazon'] = df_products.loc[mask_amazon, 'brand_rules_push_to_amazon']
            
            # Update Push to Website only if brand rule value is valid
            mask_website = mask & df_products['brand_rules_push_to_website'].notna() & (df_products['brand_rules_push_to_website'] != '') & (df_products['brand_rules_push_to_website'] != 'nan')
            df_products.loc[mask_website, 'Push to Website'] = df_products.loc[mask_website, 'brand_rules_push_to_website']
            
            # Update MAP Price only where brand_rules_map_percentage is available
            mask_map = mask & df_products['brand_rules_map_percentage'].notna() & (df_products['brand_rules_map_percentage'] != '') & (df_products['brand_rules_map_percentage'] != 'nan')
            
            # Store original MAP Price in a temporary column
            df_products['original_MAP_Price'] = df_products['MAP Price']
            
            # Calculate new MAP Price where applicable
            df_products.loc[mask_map, 'MAP Price'] = (1 - df_products.loc[mask_map, 'brand_rules_map_percentage'].astype(float)) * df_products.loc[mask_map, 'Retail Price'].astype(float)
            df_products['MAP Price'] = df_products['MAP Price'].round(2)
            
            # Where brand_rules_map_percentage is not applicable, keep the original MAP Price
            df_products.loc[~mask_map, 'MAP Price'] = df_products.loc[~mask_map, 'original_MAP_Price']

            #print all MAP Price values that are not 0, nan or ''
            print(df_products[(df_products['MAP Price'] != '0.0') & (df_products['MAP Price'] != 'nan') & (df_products['MAP Price'] != '')][['P1Moto SKU', 'MAP Price']].head(10))
            
            # Drop temporary columns
            df_products = df_products.drop(columns=['normalized_brand', 'brand_rules_brand_name', 'brand_rules_map_percentage', 'brand_rules_push_to_amazon', 'brand_rules_push_to_website', 'original_MAP_Price'])

        #---------------------------------------------End Brand Rules Logic
        # Drop rows where ASIN is '' or 'NOTAVAILABLE'
        df_products = df_products[~df_products['ASIN'].isin(['', 'NOTAVAILABLE'])]
        #drop rows where ASIN == 'NOTAVAILABLE' or ''
        df_products = df_products[df_products['ASIN'] != 'NOTAVAILABLE']
        df_products = df_products[df_products['ASIN'] != '']
        #drop rows where ASIN isn't nan or null
        df_products = df_products[df_products['ASIN'].notna()]

        logging.info(f"Number of products before Push to Amazon filtering: {len(df_products)}")
        # Drop rows where Push to Amazon is No
        df_products = df_products[df_products['Push to Amazon'] != 'No']
        logging.info(f"Number of products after Push to Amazon filtering: {len(df_products)}")
        # After all filtering, get the remaining SKUs
        remaining_skus = df_products['P1Moto SKU'].tolist()

        # Fetch additional data from P1Moto table
        p1moto_data = get_p1moto_data(connection, remaining_skus)

        # Identify new columns (columns in p1moto_data that are not in df_products)
        new_columns = [col for col in p1moto_data.columns if col not in df_products.columns]

        # Merge only the new columns from p1moto_data into df_products
        if new_columns:
            df_products = pd.merge(
                df_products, 
                p1moto_data[['P1Moto SKU'] + new_columns], 
                on='P1Moto SKU', 
                how='left'
            )
        '''
        if 'brand_rules_push_to_amazon' in df_products.columns:
            df_products.loc[
                df_products['brand_rules_push_to_amazon'].notna() &
                (df_products['brand_rules_push_to_amazon'] != ''),
                'Push to Amazon'
            ] = df_products['brand_rules_push_to_amazon']
            print("Successfully used brand_rules_push_to_amazon value")

        df_products = df_products.drop(columns=[
            'normalized_brand', 'brand_rules_brand_name', 'brand_rules_map_percentage',
            'brand_rules_push_to_amazon', 'brand_rules_push_to_website', 'original_MAP_Price'
        ], errors='ignore')
        '''
        df_products = df_products[df_products['Push to Amazon'] != 'No']
        
        # Calculate price_to_push
        df_products['price_to_push'] = df_products.apply(
            lambda row: calculate_price_to_push(
                row['Fulfillment Cost'],
                row['SellerFulfilledReferralFee'],
                row['Requested Margin']
            ),
            axis=1
        )

        # Apply MAP Price logic
        df_products['MAP Price'] = pd.to_numeric(df_products['MAP Price'], errors='coerce')
        mask = (df_products['MAP Price'] > 0) & (df_products['MAP Price'] > df_products['price_to_push'])
        df_products.loc[mask, 'price_to_push'] = df_products.loc[mask, 'MAP Price']

        # Read overwrite data
        df_overwrite = get_dataframe(connection, f"seller_products_overwrite_{user_id}")

        # Keep only 'P1Moto SKU' and 'MAP Price' columns
        df_overwrite_map = df_overwrite[['P1Moto SKU', 'MAP Price']]

        # Merge overwrite MAP Price into df_products
        df_products = pd.merge(
            df_products,
            df_overwrite_map,
            on='P1Moto SKU',
            how='left',
            suffixes=('', '_overwrite')
        )

        # Apply overwrite MAP Price logic
        # Convert 'MAP Price_overwrite' to numeric
        df_products['MAP Price_overwrite'] = pd.to_numeric(df_products['MAP Price_overwrite'], errors='coerce')

        # Check if overwrite MAP Price is valid and greater than current price_to_push
        mask = (
            df_products['MAP Price_overwrite'].notna() &
            (df_products['MAP Price_overwrite'] != '') &
            (df_products['MAP Price_overwrite'] > df_products['price_to_push'])
        )

        # Update price_to_push where conditions are met
        df_products.loc[mask, 'price_to_push'] = df_products.loc[mask, 'MAP Price_overwrite']

        # Clean up temporary columns
        df_products = df_products.drop(columns=['MAP Price_overwrite'])
        
        # Apply overwrite logic for price_to_push
        for index, row in df_overwrite.iterrows():
            sku = row['P1Moto SKU']
            if sku in df_products['P1Moto SKU'].values and 'price_to_push' in row:
                if pd.notna(row['price_to_push']) and row['price_to_push'] != '' and row['price_to_push'] is not None and row['price_to_push'] != 'nan' and row['price_to_push'] != 'NULL' and row['price_to_push'] != 'None':
                    try:
                        overwrite_price = float(row['price_to_push'])
                        df_products.loc[df_products['P1Moto SKU'] == sku, 'price_to_push'] = overwrite_price
                    except ValueError:
                        logging.warning(f"Invalid price_to_push value in overwrite table for SKU {sku}. Price to push value is {row['price_to_push']}")

        # Convert price_to_push back to string
        df_products['price_to_push'] = df_products['price_to_push'].astype(str)

        #drop rows where price_to_push isn't nan or null or ''
        df_products = df_products[df_products['price_to_push'].notna()]
        df_products = df_products[df_products['price_to_push'] != '']
        print(f"Length of df_products is {len(df_products)}")
        df_products = create_product_hash(df_products)
        return df_products
    except Exception as e:
        logging.error(f"Error processing data for user {user_id}: {str(e)}")
        logging.exception("Traceback:")
        return None

async def getSkusToDelete(df_products, user_id, access_token, refresh_token):
    global global_amazon_skus
    try:
        # Initialize the Reports API client with user-specific credentials
        reports_api = Reports(
            credentials={
                'refresh_token': refresh_token,
                'lwa_app_id': os.getenv('CLIENT_ID'),
                'lwa_client_secret': os.getenv('CLIENT_SECRET'),
                'aws_secret_key': os.getenv('SP_API_SECRET_KEY'),
                'aws_access_key': os.getenv('SP_API_ACCESS_KEY'),
                'role_arn': os.getenv('SP_API_ROLE_ARN'),
            },
            marketplace=Marketplaces.US  # Assuming US marketplace, adjust if needed
        )
        
        # Request the All Listings Report
        create_report_response = reports_api.create_report(
            reportType=ReportType.GET_MERCHANT_LISTINGS_ALL_DATA,
            marketplaceIds=[Marketplaces.US.marketplace_id]
        )
        
        #logging.info(f"Create report response: {create_report_response}")
        report_id = create_report_response.payload.get('reportId')
        logging.info(f"Report ID: {report_id}")
        
        if not report_id:
            logging.error("Failed to create report. No report ID received.")
            return []
        
        # Check report status until it's done
        max_attempts = 30  # Increased to allow for longer processing time
        attempt = 0
        while attempt < max_attempts:
            report_status = reports_api.get_report(report_id)
            logging.info(f"Report status: {report_status}")
            if report_status.payload.get('processingStatus') == 'DONE':
                break
            elif report_status.payload.get('processingStatus') in ['CANCELLED', 'FATAL']:
                logging.error(f"Report processing failed for user {user_id}: {report_status.payload.get('processingStatus')}")
                return []
            attempt += 1
            time.sleep(30)  # Wait for 30 seconds before checking again
        
        if attempt == max_attempts:
            logging.error(f"Report processing timed out after {max_attempts} attempts")
            return []
        
        # Get the report document
        report_document_id = report_status.payload.get('reportDocumentId')
        logging.info(f"Report document ID: {report_document_id}")
        
        if not report_document_id:
            logging.error("No report document ID found in the report status.")
            return []
        
        report_document = reports_api.get_report_document(report_document_id, decrypt=True)
        #logging.info(f"Report document response: {report_document}")
        
        if not report_document or not report_document.payload or 'url' not in report_document.payload:
            logging.error("Failed to retrieve the report document or URL is missing.")
            return []
        
        document_url = report_document.payload['url']
        logging.info(f"Document URL: {document_url}")

        # Fetch the content from the URL
        response = requests.get(document_url)
        if response.status_code != 200:
            logging.error(f"Failed to fetch document content. Status code: {response.status_code}")
            return []

        # The content is always gzipped based on the information in the payload
        compression_algorithm = report_document.payload.get('compressionAlgorithm')

        try:
            if compression_algorithm == 'GZIP':
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                    content = gz_file.read()
            else:
                content = response.content

            # Detect encoding
            detected_encoding = chardet.detect(content)['encoding']
            
            # Decode content
            document_content = content.decode(detected_encoding or 'utf-8', errors='replace')

            # Remove null bytes if present
            document_content = document_content.replace('\x00', '')
        except Exception as e:
            logging.error(f"Error processing content: {str(e)}")
            return []

        logging.info(f"First 50 characters of document: {document_content[:50]}")

        # Parse the tab-delimited content
        csv_reader = csv.DictReader(io.StringIO(document_content), delimiter='\t')
        
        amazon_skus = set()
        for row in csv_reader:
            sku = row.get('seller-sku')
            if sku:
                amazon_skus.add(sku)
        global_amazon_skus = amazon_skus
        logging.info(f"Number of Amazon SKUs: {len(amazon_skus)}")

        if df_products is None:
            # If no products are provided, mark all Amazon SKUs for deletion
            return list(amazon_skus)
        
        # Get the list of SKUs in our filtered DataFrame
        df_skus = set(df_products['P1Moto SKU'].tolist())
        logging.info(f"Number of DataFrame SKUs: {len(df_skus)}")

        target_sku = 'P1102947539120'
        if target_sku in amazon_skus:
            logging.info(f"SKU {target_sku} is present in Amazon SKUs")
        else:
            logging.info(f"SKU {target_sku} is NOT present in Amazon SKUs")
        
        # Find SKUs to delete (in Amazon but not in our DataFrame)
        skus_to_delete = [sku for sku in amazon_skus if sku not in df_skus]

        if skus_to_delete:
            print(f"Number of SKUs to delete: {len(skus_to_delete)}")
            logging.info(f"Found {len(skus_to_delete)} SKUs to delete for user {user_id}")
            logging.info(f"First 10 SKUs to delete: {skus_to_delete[:10]}")
        
        return skus_to_delete
        
    except Exception as e:
        logging.error(f"Error in getSkusToDelete for user {user_id}: {str(e)}")
        logging.exception("Traceback:")
        return []

async def exponential_backoff(attempt, max_attempts=20, base_delay=5, max_delay=300):
    if attempt >= max_attempts:
        raise Exception("Max retry attempts reached")
    delay = min(max_delay, base_delay * (2 ** attempt) + random.uniform(0, 1))
    await asyncio.sleep(delay)

class AmazonDataProcessor:
    def __init__(self, user_id, access_token, refresh_token, selling_partner_id, connection):
        self.user_id = user_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.selling_partner_id = selling_partner_id
        self.connection = connection
        self.batch_id = None
        self.sku_to_message_id = None
        self.all_messages = []

    async def create_and_push(self, df_products, skus_to_delete):
        try:
            change_history_table = Table(f'change_history_{self.user_id}', MetaData(), autoload_with=self.connection)
            updated_prices = {}
            feeds_api = Feeds(
                credentials={
                    'refresh_token': self.refresh_token,
                    'lwa_app_id': os.getenv('CLIENT_ID'),
                    'lwa_client_secret': os.getenv('CLIENT_SECRET'),
                    'aws_secret_key': os.getenv('SP_API_SECRET_KEY'),
                    'aws_access_key': os.getenv('SP_API_ACCESS_KEY'),
                    'role_arn': os.getenv('SP_API_ROLE_ARN'),
                },
                marketplace=Marketplaces.US
            )

            chunk_size = 10000
            total_products = len(df_products)
            total_chunks = (total_products + len(skus_to_delete) + chunk_size - 1) // chunk_size

            feed_ids = []

            for chunk_index in range(total_chunks):
                start_index = chunk_index * chunk_size
                end_index = min(start_index + chunk_size, total_products)

                feed_content = {
                    "header": {
                        "sellerId": self.selling_partner_id,
                        "version": "2.0",
                        "issueLocale": "en_US"
                    },
                    "messages": []
                }

                # Process product updates
                for index, row in df_products.iloc[start_index:end_index].iterrows():
                    try:
                        price_to_push = round(float(row['price_to_push']), 2)
                        retail_price = round(float(row['Retail Price']), 2)
                        list_price = max(price_to_push, retail_price)

                        # Check if number_of_items is valid and greater than UOM
                        if (pd.notna(row['number_of_items']) and row['number_of_items'] != '' and pd.notna(row['UOM']) and row['UOM'] != ''):
                            number_of_items = int(float(row['number_of_items']))
                            UOM = int(float(row['UOM']))
                            
                            if number_of_items > UOM and UOM != 0:  # Prevent division by zero
                                multiple = number_of_items / UOM  # This will be a float
                                shipping_cost = float(row['Fulfillment Cost']) - float(row['Cost'])
                                new_fulfillment_cost = (float(row['Cost']) * multiple) + shipping_cost
                                
                                # Recalculate price_to_push with new_fulfillment_cost
                                new_price_to_push = calculate_price_to_push(
                                    new_fulfillment_cost,
                                    float(row['SellerFulfilledReferralFee']),
                                    float(row['Requested Margin'])
                                )
                                
                                if pd.notna(new_price_to_push):
                                    if row['P1Moto SKU'] == 'P1746976556771':
                                        print(f"Old price: {price_to_push}, New price: {new_price_to_push}")
                                    price_to_push = max(round(new_price_to_push, 2), retail_price)
                                    updated_prices[row['P1Moto SKU']] = price_to_push

                    except ValueError:
                        logging.warning(f"Invalid price for SKU {row['P1Moto SKU']}")
                        continue


                    product_type = row.get('product_type', 'SPORTING_GOODS')
                    item_name = row.get('asin_item_name', str(row['Title']))
                    message_id = self.sku_to_message_id[row['P1Moto SKU']]

                    message = {
                        "messageId": int(message_id),
                        "sku": str(row['P1Moto SKU']),
                        "operationType": "UPDATE",
                        "productType": product_type,
                        "requirements": "LISTING_OFFER_ONLY",
                        "attributes": {
                            "list_price": [{"value": list_price}],
                            "item_name": [{"value": item_name, "language_tag": "en_US"}],
                            "condition_type": [{"value": "new_new"}],
                            "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": int(float(row['Quantity']))}],
                            "purchasable_offer": [{
                                "marketplace_id": "ATVPDKIKX0DER",
                                "currency": "USD",
                                "our_price": [{"schedule": [{"value_with_tax": price_to_push}]}]
                            }]
                        }
                    }

                    if pd.notna(row['ASIN']) and len(str(row['ASIN'])) == 10:
                        message["attributes"]["merchant_suggested_asin"] = [{"value": str(row['ASIN'])}]

                    feed_content["messages"].append(message)
                    self.all_messages.append({"messageId": int(message_id), "sku": row['P1Moto SKU']})

                # Process SKUs to delete
                delete_start = max(0, chunk_index * chunk_size - total_products)
                delete_end = min(len(skus_to_delete), delete_start + (chunk_size - len(feed_content["messages"])))

                for sku in skus_to_delete[delete_start:delete_end]:
                    message_id = self.sku_to_message_id[sku]
                    message = {
                        "messageId": int(message_id),
                        "sku": sku,
                        "operationType": "DELETE"
                    }
                    feed_content["messages"].append(message)
                    self.all_messages.append({"messageId": message_id, "sku": sku})

                if skus_to_delete[delete_start:delete_end]:
                    # Delete SKUs from change history table
                    delete_stmt = change_history_table.delete().where(
                        change_history_table.c.P1Moto_SKU.in_(skus_to_delete[delete_start:delete_end])
                    )
                    result = self.connection.execute(delete_stmt)
                    logging.info(f"Deleted {result.rowcount} SKUs from change history for user {self.user_id}")
                # Submit the feed with retries
                max_attempts = 20
                for attempt in range(max_attempts):
                    try:
                        feed_id = await self.submit_feed_chunk(feeds_api, feed_content, chunk_index)
                        feed_ids.append(feed_id)
                        break
                    except Exception as e:
                        if 'QuotaExceeded' in str(e) and attempt < max_attempts - 1:
                            logging.warning(f"QuotaExceeded for user {self.user_id}, chunk {chunk_index + 1}. Retrying...")
                            await exponential_backoff(attempt)
                        else:
                            raise

                await asyncio.sleep(2 * 60)  # Wait for 12 minutes between chunks
                # Batch update change history table after processing each chunk
                if updated_prices:
                    try:
                        update_stmt = change_history_table.update().where(
                            change_history_table.c.P1Moto_SKU == bindparam('sku')
                        ).values(price=bindparam('price'))
                                
                        self.connection.execute(update_stmt, [
                            {'sku': sku, 'price': price} for sku, price in updated_prices.items()
                        ])
                        logging.info(f"Updated {len(updated_prices)} prices in change history for user {self.user_id}")
                    except Exception as e:
                        logging.error(f"Error updating prices in change history for user {self.user_id}: {str(e)}")
                    finally:
                        updated_prices.clear()

            results = await asyncio.gather(*[self.check_feed_status(feeds_api, feed_id) for feed_id in feed_ids])

            processed_results = []
            for result in results:
                if isinstance(result, dict) and 'error' not in result:
                    processed_results.append(result)
                else:
                    logging.error(f"Error processing feed: {result.get('error', 'Unknown error')}")

            return processed_results

        except Exception as e:
            logging.error(f"Error in create_and_push for user {self.user_id}: {str(e)}")
            return [{"messageId": msg["messageId"], "status": "ERROR", "errorMessage": str(e)} for msg in self.all_messages]
    
    async def submit_feed_chunk(self, feeds_api, feed_content, chunk_index):
        logging.info(f"Submitting chunk {chunk_index + 1} for user {self.user_id}")
        feed_json = json.dumps(feed_content)

        create_feed_document_response = feeds_api.create_feed_document(
            file=StringIO(feed_json),
            content_type='application/json'
        )

        feed_document_id = create_feed_document_response.payload.get('feedDocumentId')
        url = create_feed_document_response.payload.get('url')

        # Use ThreadPoolExecutor for I/O-bound operations
        with ThreadPoolExecutor() as executor:
            response = await asyncio.get_event_loop().run_in_executor(
                executor, 
                lambda: requests.put(url, data=feed_json, headers={'Content-Type': 'application/json'})
            )
            if response.status_code != 200:
                raise Exception(f"Failed to upload feed document: {response.text}")

        create_feed_response = feeds_api.create_feed(
            feed_type='JSON_LISTINGS_FEED',
            input_feed_document_id=feed_document_id,
            marketplaceIds=[Marketplaces.US.marketplace_id]
        )

        if create_feed_response.errors:
            raise Exception(create_feed_response.errors)

        feed_id = create_feed_response.payload.get('feedId')
        logging.info(f"Feed chunk {chunk_index + 1} submitted for user {self.user_id}. Feed ID: {feed_id}")
        return feed_id

    async def check_feed_status(self, feeds_api, feed_id):
        max_attempts = 150
        attempt = 0
        while attempt < max_attempts:
            try:
                logging.info(f"Checking status of feed {feed_id}. Attempt: {attempt + 1}")
                feed_response = feeds_api.get_feed(feed_id)
                processing_status = feed_response.payload.get('processingStatus')
                logging.info(f"Feed {feed_id} status: {processing_status}")

                if processing_status == 'DONE':
                    logging.info(f"Processing for feed {feed_id} is done.")
                    result_feed_document_id = feed_response.payload.get('resultFeedDocumentId')
                    if result_feed_document_id:
                        result_document = feeds_api.get_feed_document(result_feed_document_id)
                        return self.process_feed_result(result_document, feed_id)
                    else:
                        return {'error': f"No result document for feed {feed_id}"}
                elif processing_status in ['CANCELLED', 'FATAL']:
                    error_msg = f"Feed {feed_id} processing failed with status {processing_status}"
                    logging.error(error_msg)
                    return {'error': error_msg}
            
                attempt += 1
                await exponential_backoff(attempt, max_attempts=150, base_delay=30, max_delay=300)
            except Exception as e:
                logging.error(f"Error checking status of feed {feed_id}: {str(e)}")
                attempt += 1
                await exponential_backoff(attempt, max_attempts=150, base_delay=30, max_delay=300)

        error_msg = f"Feed {feed_id} processing timed out after {max_attempts} attempts"
        logging.error(error_msg)
        return {'error': error_msg}

    def process_feed_result(self, result_document, feed_id):
        try:
            result_json = json.loads(result_document) if isinstance(result_document, str) else result_document
            
            if 'header' in result_json and 'issues' in result_json:
                issues = result_json['issues']
                summary = result_json.get('summary', {})
                processed_results = []

                grouped_issues = defaultdict(list)
                for issue in issues:
                    grouped_issues[str(issue.get('messageId', ''))].append(issue)

                for message_id, message_issues in grouped_issues.items():
                    sku = next((msg['sku'] for msg in self.all_messages if msg['messageId'] == int(message_id)), None)
                    status = 'SUCCESS' if all(issue.get('severity') != 'ERROR' for issue in message_issues) else 'ERROR'
                    error_message = '; '.join(f"{issue.get('attributeName', 'Unknown attribute')}: {issue.get('message', 'No message')}" 
                                            for issue in message_issues if issue.get('severity') == 'ERROR')

                    processed_results.append({
                        "messageId": int(message_id),
                        "sku": sku,
                        "status": status,
                        "errorMessage": error_message if status == 'ERROR' else ''
                    })

                return {
                    'results': processed_results,
                    'summary': {
                        'messagesProcessed': summary.get('messagesProcessed', 0),
                        'messagesSuccessful': summary.get('messagesAccepted', 0),
                        'messagesFailed': summary.get('messagesInvalid', 0)
                    }
                }
            else:
                return {'error': f"Unexpected JSON structure in feed {feed_id} result"}

        except Exception as e:
            error_msg = f"Error processing feed result for feed {feed_id}: {str(e)}"
            logging.error(error_msg)
            logging.exception("Exception details:")
            return {'error': error_msg}

async def process_user(connection, user, results):
    global global_amazon_skus
    user_id = user['id']
    access_token = user['access_token']
    refresh_token = user['refresh_token']
    selling_partner_id = user['selling_partner_id']

    processor = AmazonDataProcessor(user_id, access_token, refresh_token, selling_partner_id, connection)
    
    try:
        logging.info(f"Processing user ID: {user_id}")

        # Check lock status before processing each user
        if not check_lock_status(connection):
            logging.error(f"Lock lost while processing user {user_id}. Terminating.")
            raise Exception("Lock lost")

        check_and_recreate_change_history_table(connection, user_id)
        ensure_product_hash_column(connection, user_id)

        df_result = process_user_data(connection, user_id)
        if df_result is not None and not df_result.empty:
            cleanup_change_history_table(connection, user_id, df_result['P1Moto SKU'].tolist())
            skus_to_delete = await getSkusToDelete(df_result, user_id, access_token, refresh_token)
            
            print(f"Length of df_result before filtering: {len(df_result)}")
            
            # Filter unchanged products
            df_result = filter_unchanged_products(connection, user_id, df_result)
            
            print(f"Length of df_result after filtering: {len(df_result)}")
            
            # Initialize the process and get batch_id and sku_to_message_id mapping
            processor.batch_id, processor.sku_to_message_id = comprehensive_amazon_data_process(
                connection, user_id, selling_partner_id, df_result, skus_to_delete
            )

            # Perform Amazon API operations
            amazon_results = await processor.create_and_push(df_result, skus_to_delete)

            # Update the database with the results
            logging.info(f"Updating process results for user {user_id}")
            final_results = update_amazon_process_results(
                connection, user_id, processor.batch_id, processor.sku_to_message_id, amazon_results
            )

            results[user_id] = final_results
            logging.info(f"Processing completed for user {user_id}: {final_results}")
        else:
            skus_to_delete = await getSkusToDelete(None, user_id, access_token, refresh_token)
            if skus_to_delete:
                processor.batch_id, processor.sku_to_message_id = comprehensive_amazon_data_process(
                    connection, user_id, selling_partner_id, pd.DataFrame(), skus_to_delete
                )
                amazon_results = await processor.create_and_push(pd.DataFrame(), skus_to_delete)
                final_results = update_amazon_process_results(
                    connection, user_id, processor.batch_id, processor.sku_to_message_id, amazon_results
                )
                results[user_id] = final_results
                logging.info(f"Processing completed for user {user_id}: {final_results}")
            logging.warning(f"No data processed for user ID {user_id}")
            results[user_id] = "No data processed"

    except Exception as user_error:
        logging.error(f"Error processing user {user_id}: {str(user_error)}")
        logging.exception("Traceback:")
        results[user_id] = f"Error: {str(user_error)}"

    return results

def cleanup_change_history_table(connection, user_id, valid_skus):
    try:
        change_history_table = Table(f'change_history_{user_id}', MetaData(), autoload_with=connection)
        
        # Delete rows where P1Moto SKU is not in the valid_skus list
        delete_stmt = change_history_table.delete().where(
            ~change_history_table.c.P1Moto_SKU.in_(valid_skus)
        )
        result = connection.execute(delete_stmt)
        
        logging.info(f"Cleaned up change history table for user {user_id}. Deleted {result.rowcount} rows.")
    except Exception as e:
        logging.error(f"Error cleaning up change history table for user {user_id}: {str(e)}")
        logging.exception("Traceback:")

async def periodic_extend_lock(connection):
    while True:
        await asyncio.sleep(30 * 60)  # Extend every 30 minutes
        if not extend_lock(connection):
            logging.warning("Failed to extend lock. The script may be terminated soon.")

async def main_async(specific_user_id=None):
    # Set up logging to file
    log_file = 'last_async_run.log'
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Get the root logger and add the file handler
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    tunnel = None
    engine = None
    connection = None
    extend_lock_task = None
    results = defaultdict(dict)
    start_time = datetime.now()
    lock_acquired = False

    try:
        logging.info(f"Script started at {start_time}")

        tunnel, engine = get_db_connection()
        connection = engine.connect()

        if not acquire_lock(connection):
            logging.error("Another instance of this script is already running. Exiting.")
            return

        lock_acquired = True
        lock_renewal_task = asyncio.create_task(periodic_lock_renewal(connection))

        users = get_all_user_ids(connection, specific_user_id)

        if not users:
            logging.error(f"No users found" + (f" for ID: {specific_user_id}" if specific_user_id else ""))
            return

        # Create tasks for all users (or just the specific user)
        tasks = [process_user(connection, user, results) for user in users]

        # Run all tasks concurrently
        logging.info("Starting to process all users")
        await asyncio.gather(*tasks)
        logging.info("Finished processing all users")

        # After all users are processed, analyze and log the results
        logging.info("Starting to analyze results")
        analyze_results(results)
        logging.info("Finished analyzing results")

        # Log successful completion
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Script completed successfully at {end_time}")
        logging.info(f"Total runtime: {duration}")

        # Update the log file with the last successful run time
        with open(log_file, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(f"Last successful run: {end_time}\n\n" + content)

    except Exception as e:
        logging.error(f"An error occurred in main_async: {str(e)}")
        logging.exception("Traceback:")
    finally:
        if 'lock_renewal_task' in locals():
            lock_renewal_task.cancel() 
        if connection:
            if lock_acquired:  # Only release the lock if we acquired it
                release_lock(connection)
            connection.close()
        if engine:
            engine.dispose()
        if tunnel:
            tunnel.close()
        logging.info("Cleaned up resources in main_async")

        # Remove the file handler from the root logger
        root_logger.removeHandler(file_handler)

    logging.info("Exiting main_async function")


def analyze_results(results):
    total_users = len(results)
    successful_users = sum(1 for result in results.values() if isinstance(result, dict) and result.get('status') == 'completed')
    error_users = sum(1 for result in results.values() if isinstance(result, str) and result.startswith("Error"))
    no_data_users = sum(1 for result in results.values() if result == "No data processed")
    
    logging.info(f"Processing complete for all users.")
    logging.info(f"Total users processed: {total_users}")
    logging.info(f"Successful processing: {successful_users}")
    logging.info(f"Users with errors: {error_users}")
    logging.info(f"Users with no data: {no_data_users}")
    
    for user_id, result in results.items():
        if isinstance(result, str) and result.startswith("Error"):
            logging.error(f"User {user_id}: {result}")
        elif result == "No data processed":
            logging.warning(f"User {user_id}: No data processed")
        elif isinstance(result, dict):
            logging.info(f"User {user_id}: Processing completed with {result.get('successful_messages', 0)} successful messages and {result.get('failed_messages', 0)} failed messages")
        else:
            logging.warning(f"User {user_id}: Unexpected result format")

if __name__ == "__main__":
    user_id = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main_async(user_id))