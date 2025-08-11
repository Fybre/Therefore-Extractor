import argparse
import json
import logging
import os
import sqlite3
import time
import datetime

DEFAULT_CONFIG_PATH = 'config/config.json'
DEFAULT_DB_DIR = 'db/docs'
DEFAULT_TENANT_NAME = 'defaulttenant'

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def get_therefore_documents_for_processing(config_path=DEFAULT_CONFIG_PATH, tenant_name=None, db_dir=DEFAULT_DB_DIR):
    import therefore_functions
    import utils

    # Load configuration from config.json
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        return
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing configuration file: {e}")
        return

    # Filter tenants if requested
    if tenant_name:
        tenant_config = next((tenant for tenant in config['Tenants'] if tenant.get('Tenant') == tenant_name), None)
        if not tenant_config:
            logging.error(f"Tenant '{tenant_name}' not found in configuration.")
            return
        tenant_configs = [tenant_config]
    else:
        tenant_configs = config['Tenants']

    # Grand totals
    grand_total_docs = 0
    grand_total_time = 0
    tenant_count = 0

    for tenant_config in tenant_configs:
        tenant_name_conf = tenant_config.get('Tenant', DEFAULT_TENANT_NAME)
        logging.info(f"Processing tenant: {tenant_name_conf}")
        tenant_start = time.time()
        doc_count = 0

        # Auth
        auth_token = utils.basic_auth_token(tenant_config['Username'], tenant_config['Password'])

        # Fetch categories for tenant
        try:
            category_list = therefore_functions.get_all_categories(
                tenant_config['BaseUrl'],
                tenant_config['Tenant'],
                auth_token
            )
        except Exception as e:
            logging.error(f"Failed to get categories for tenant '{tenant_name_conf}': {e}")
            continue

        # Prepare tenant DB
        os.makedirs(db_dir, exist_ok=True)
        db_path = os.path.join(db_dir, f"{tenant_name_conf}.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                DocNo INTEGER PRIMARY KEY,
                Version INTEGER,
                Data TEXT
            )
        ''')
        conn.commit()

        # Process each category
        for category in category_list:
            if tenant_config.get('categories') and category['ItemNo'] not in tenant_config['categories']:
                logging.info(f"Skipping category {category['Name']} (ItemNo: {category['ItemNo']}) "
                             "as it is not in the tenant's category list for processing.")
                continue

            logging.info(f"Category No: {category['ItemNo']}, Name: {category['Name']}")

            try:
                rows = therefore_functions.query_all_category_documents(
                    tenant_config['BaseUrl'],
                    tenant_config['Tenant'],
                    auth_token,
                    category_no=category['ItemNo']
                )
            except Exception as e:
                logging.error(f"Failed to fetch documents for category {category['Name']}: {e}")
                continue

            logging.info(f"Total rows fetched for category {category['Name']}: {len(rows)}")

            for row in rows:
                doc_no = row.get('DocNo')
                version = row.get('VersionNo')

                cursor.execute('SELECT Version FROM documents WHERE DocNo=?', (doc_no,))
                result = cursor.fetchone()

                if result is None or result[0] != version:
                    cursor.execute(
                        'REPLACE INTO documents (DocNo, Version, Data) VALUES (?, ?, ?)',
                        (doc_no, version, json.dumps(row))
                    )
                    logging.info(f"Processed DocNo: {doc_no}, Version: {version}")
                    doc_count += 1
                else:
                    logging.debug(f"Skipped DocNo: {doc_no}, Version: {version} (already up to date)")

            conn.commit()

        conn.close()

        tenant_elapsed = time.time() - tenant_start
        avg_time = tenant_elapsed / doc_count if doc_count > 0 else 0
        logging.info(
            f"Tenant '{tenant_name_conf}' summary: processed {doc_count} document(s) "
            f"in {str(datetime.timedelta(seconds=int(tenant_elapsed)))} "
            f"(avg {avg_time:.2f} sec/doc)"
        )

        # Accumulate grand totals
        tenant_count += 1
        grand_total_docs += doc_count
        grand_total_time += tenant_elapsed

    # Grand summary
    if tenant_count > 0 and grand_total_docs > 0:
        avg_time_overall = grand_total_time / grand_total_docs
        logging.info(
            f"GRAND TOTAL: processed {grand_total_docs} document(s) "
            f"across {tenant_count} tenant(s) in {str(datetime.timedelta(seconds=int(grand_total_time)))} "
            f"(avg {avg_time_overall:.2f} sec/doc)"
        )


# ----------------------------
# Main entry point
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Therefore documents for tenants.")
    parser.add_argument("--tenant", dest="tenant_name", help="Name of the tenant to process")
    parser.add_argument("--config", dest="config_path", default=DEFAULT_CONFIG_PATH, help="Path to config file")
    parser.add_argument("--db-dir", dest="db_dir", default=DEFAULT_DB_DIR, help="Directory for SQLite DBs")
    args = parser.parse_args()

    run_start = time.time()
    get_therefore_documents_for_processing(
        config_path=args.config_path,
        tenant_name=args.tenant_name,
        db_dir=args.db_dir,
    )
    run_elapsed = time.time() - run_start
    logging.info(f"Total wall-clock run time: {str(datetime.timedelta(seconds=int(run_elapsed)))} (hh:mm:ss)")
