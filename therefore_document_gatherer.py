import argparse

DEFAULT_CONFIG_PATH = 'config/config.json'
DEFAULT_DB_DIR = 'db/docs'
DEFAULT_TENANT_NAME = 'defaulttenant'

def get_therefore_documents_for_processing(config_path=DEFAULT_CONFIG_PATH, tenant_name=None, db_dir=DEFAULT_DB_DIR):
    import json
    import therefore_functions
    import utils
    import sqlite3
    import os

    # Load configuration from config.json
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)

    # If tenant_name is provided, find the corresponding configuration
    if tenant_name:
        tenant_config = next((tenant for tenant in config['Tenants'] if tenant.get('Tenant') == tenant_name), None)
        if not tenant_config:
            raise ValueError(f"Tenant '{tenant_name}' not found in configuration.")
        tenant_configs = [tenant_config]
    else:
        tenant_configs = config['Tenants']

    for tenant_config in tenant_configs:
        print(f"Processing tenant: {tenant_config.get('Tenant', DEFAULT_TENANT_NAME)}")
        auth_token = utils.basic_auth_token(tenant_config['Username'], tenant_config['Password'])

        category_list = therefore_functions.get_all_categories(
            tenant_config['BaseUrl'],  
            tenant_config['Tenant'],
            auth_token
        )

        # Set up SQLite database and table
        os.makedirs(db_dir, exist_ok=True)
        tenant = tenant_config.get('Tenant') or DEFAULT_TENANT_NAME
        db_path = f"{db_dir}/{tenant}.db"
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

        for category in category_list:
            if tenant_config.get('categories') and category['ItemNo'] not in tenant_config['categories']:
                print(f"Skipping category {category['Name']} (ItemNo: {category['ItemNo']}) as it is not in the tenant's category list for processing.")
                continue
            print(f"Category No: {category['ItemNo']}, Name: {category['Name']}")
            rows = therefore_functions.query_all_category_documents(
                tenant_config['BaseUrl'],
                tenant_config['Tenant'],
                auth_token,
                category_no=category['ItemNo']
            )
            print(f"\tTotal rows fetched for category {category['Name']}: {len(rows)}")

            for row in rows:
                doc_no = row.get('DocNo')
                version = row.get('VersionNo')
                cursor.execute('SELECT Version FROM documents WHERE DocNo=?', (doc_no,))
                result = cursor.fetchone()
                if result is None or result[0] != version:
                    cursor.execute('REPLACE INTO documents (DocNo, Version, Data) VALUES (?, ?, ?)',
                                   (doc_no, version, json.dumps(row)))
                    print(f"\tProcessed DocNo: {doc_no}, Version: {version}")
                else:
                    print(f"\tSkipped DocNo: {doc_no}, Version: {version} (already up to date)")

            conn.commit()

        conn.close()

# To run directly:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Therefore documents for tenants.")
    parser.add_argument("--tenant", dest="tenant_name", help="Name of the tenant to process")
    parser.add_argument("--config", dest="config_path", default=DEFAULT_CONFIG_PATH, help="Path to config file")
    parser.add_argument("--db-dir", dest="db_dir", default=DEFAULT_DB_DIR, help="Directory for SQLite DBs")
    args = parser.parse_args()

    get_therefore_documents_for_processing(
        config_path=args.config_path,
        tenant_name=args.tenant_name,
        db_dir=args.db_dir,
    )