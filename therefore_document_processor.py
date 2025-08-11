import argparse
import json
import logging
import os
import sqlite3
import time
import datetime
from typing import List

import numpy as np
import chromadb
from chromadb.config import Settings
from nomic import embed

import therefore_functions
import utils

# ----------------------------
# Defaults & Config
# ----------------------------
DEFAULT_CONFIG_PATH = 'config/config.json'
DEFAULT_DB_DIR = 'db/docs'
DEFAULT_VECTORDB_DIR = 'db/vectordb'
DEFAULT_TENANT_NAME = 'defaulttenant'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# Embedding & Chunking
# ----------------------------
def _create_embeddings_batch(texts: List[str], embedding_model="nomic-embed-text-v1.5"):
    """Batch create embeddings for a list of text chunks."""
    output = embed.text(
        texts=texts,
        model=embedding_model,
        inference_mode="local",
        device="cpu",
        task_type="search_document"
    )
    return output["embeddings"]

def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """Split text into overlapping chunks."""
    words = text.split()
    return [
        " ".join(words[i:i + chunk_size])
        for i in range(0, len(words), chunk_size - overlap)
    ]

# ----------------------------
# Config loading
# ----------------------------
def load_config(config_path=DEFAULT_CONFIG_PATH):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

def get_tenant_configs(config: dict, tenant_name: str = None) -> List[dict]:
    if tenant_name:
        tenant = next((t for t in config['Tenants'] if t.get('Tenant') == tenant_name), None)
        if not tenant:
            raise ValueError(f"Tenant '{tenant_name}' not found in configuration.")
        return [tenant]
    return config['Tenants']

# ----------------------------
# Document Processing
# ----------------------------
def process_document(collection, tenant_config, auth_token, doc_no, version):
    """Convert, extract text, create chunk embeddings, and store/update in Chroma."""
    doc_id = str(doc_no)

    # Version check
    existing_chunks = collection.get(where={"parent_doc": doc_id})
    if existing_chunks["ids"]:
        stored_version = existing_chunks["metadatas"][0].get("version", 0)
        if stored_version == version:
            logging.info(f"DocNo {doc_no} already stored at version {version}. Skipping.")
            return
        else:
            collection.delete(ids=existing_chunks["ids"])
            logging.info(f"Deleted {len(existing_chunks['ids'])} old chunks for DocNo {doc_no} (old version {stored_version})")

    try:
        saved_files = therefore_functions.convert_and_save_document(
            tenant_config['BaseUrl'],
            tenant_config['Tenant'],
            auth_token,
            doc_no,
            output_dir="output",
            version=version
        )

        logging.info(f"Saved files for DocNo {doc_no}: {saved_files}")

        if not isinstance(saved_files, list) or not saved_files:
            logging.warning(f"No valid files for DocNo {doc_no}. Skipping.")
            return

        for file_path in saved_files:
            if not file_path.lower().endswith('.pdf'):
                logging.debug(f"Skipping non-PDF file: {file_path}")
                continue

            text = utils.extract_text_from_pdf(file_path)
            text_chunks = chunk_text(text)

            if not text_chunks:
                logging.warning(f"No text extracted from {file_path}. Skipping.")
                continue

            chunk_embeddings = _create_embeddings_batch(text_chunks)

            for idx, (chunk, emb) in enumerate(zip(text_chunks, chunk_embeddings)):
                chunk_vector = np.array(emb)
                collection.add(
                    ids=[f"{doc_id}_chunk{idx}"],
                    documents=[chunk],
                    embeddings=[chunk_vector],
                    metadatas=[{"version": version, "parent_doc": doc_id}]
                )

            logging.info(f"Stored {len(text_chunks)} chunks for DocNo {doc_no}, version {version}")

            os.remove(file_path)

    except Exception as e:
        logging.error(f"Failed processing DocNo {doc_no}: {e}", exc_info=True)

# ----------------------------
# Tenant Processing with summary
# ----------------------------
def process_tenant(tenant_config, db_dir, vectordb_dir):
    tenant_name = tenant_config.get('Tenant', DEFAULT_TENANT_NAME)
    logging.info(f"Processing tenant: {tenant_name}")

    tenant_start = time.time()
    doc_count = 0

    auth_token = utils.basic_auth_token(
        tenant_config['Username'],
        tenant_config['Password']
    )

    db_path = os.path.join(db_dir, f"{tenant_name}.db")
    if not os.path.exists(db_path):
        logging.warning(f"Database not found for tenant '{tenant_name}': {db_path}")
        return doc_count, 0  # return summary data

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DocNo, Version FROM documents")
    docs_to_process = cursor.fetchall()
    conn.close()

    total_docs = len(docs_to_process)
    logging.info(f"Found {total_docs} documents to process.")

    os.makedirs(vectordb_dir, exist_ok=True)
    client = chromadb.PersistentClient(
        path=os.path.join(vectordb_dir, f"{tenant_name}_chroma.db"),
        settings=Settings(anonymized_telemetry=False)
    )
    collection = client.get_or_create_collection(name="documents")

    for idx, (doc_no, version) in enumerate(docs_to_process, 1):
        doc_start = time.time()
        logging.info(f"Processing document {idx}/{total_docs} (DocNo: {doc_no}, Version: {version})")

        process_document(collection, tenant_config, auth_token, doc_no, version)
        doc_elapsed = time.time() - doc_start

        doc_count += 1
        logging.info(f"Finished DocNo {doc_no} in {doc_elapsed:.2f} seconds ({doc_elapsed/60:.2f} minutes)")

    tenant_elapsed = time.time() - tenant_start
    avg_time = tenant_elapsed / doc_count if doc_count > 0 else 0
    logging.info(
        f"Tenant '{tenant_name}' summary: processed {doc_count} document(s) "
        f"in {str(datetime.timedelta(seconds=int(tenant_elapsed)))} "
        f"(avg {avg_time:.2f} sec/doc)"
    )

    return doc_count, tenant_elapsed

# ----------------------------
# Main Entry with total timing & grand summary
# ----------------------------
def process(config_path=DEFAULT_CONFIG_PATH, tenant_name=None, db_dir=DEFAULT_DB_DIR, vectordb_dir=DEFAULT_VECTORDB_DIR):
    config = load_config(config_path)
    tenant_configs = get_tenant_configs(config, tenant_name)

    grand_total_docs = 0
    grand_total_time = 0
    tenant_count = 0

    for tenant_config in tenant_configs:
        tenant_count += 1
        docs, elapsed = process_tenant(tenant_config, db_dir=db_dir, vectordb_dir=vectordb_dir)
        grand_total_docs += docs
        grand_total_time += elapsed

    if tenant_count > 0 and grand_total_docs > 0:
        avg_time_overall = grand_total_time / grand_total_docs
        logging.info(
            f"GRAND TOTAL: processed {grand_total_docs} document(s) "
            f"across {tenant_count} tenant(s) in {str(datetime.timedelta(seconds=int(grand_total_time)))} "
            f"(avg {avg_time_overall:.2f} sec/doc)"
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and embed Therefore documents.")
    parser.add_argument("--tenant", dest="tenant_name", help="Name of the tenant to process")
    parser.add_argument("--config", dest="config_path", default=DEFAULT_CONFIG_PATH, help="Path to config file")
    parser.add_argument("--db-dir", dest="db_dir", default=DEFAULT_DB_DIR, help="Directory containing tenant SQLite DBs")
    parser.add_argument("--vectordb-dir", dest="vectordb_dir", default=DEFAULT_VECTORDB_DIR, help="Directory for Chroma persistent DBs")
    args = parser.parse_args()

    run_start = time.time()
    process(
        config_path=args.config_path,
        tenant_name=args.tenant_name,
        db_dir=args.db_dir,
        vectordb_dir=args.vectordb_dir
    )
    run_elapsed = time.time() - run_start
    logging.info(f"Total wall-clock run time: {str(datetime.timedelta(seconds=int(run_elapsed)))} (hh:mm:ss)")
