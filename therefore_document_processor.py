import argparse
import json
import logging
import os
import sqlite3
from typing import List

import numpy as np
import chromadb
from chromadb.config import Settings
from nomic import embed

import therefore_functions
import utils

# ----------------------------
# CONFIG: Logging
# ----------------------------
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
    """Split text into overlapping chunks for better retrieval performance."""
    words = text.split()
    return [
        " ".join(words[i:i + chunk_size])
        for i in range(0, len(words), chunk_size - overlap)
    ]

# ----------------------------
# Config Functions
# ----------------------------
def load_config(config_path='config.json'):
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

    # --- Version Check ---
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

            # Extract & chunk text
            text = utils.extract_text_from_pdf(file_path)
            text_chunks = chunk_text(text)

            if not text_chunks:
                logging.warning(f"No text extracted from {file_path}. Skipping.")
                continue

            # Batch embed all chunks
            chunk_embeddings = _create_embeddings_batch(text_chunks)

            # Store chunks individually in Chroma
            for idx, (chunk, emb) in enumerate(zip(text_chunks, chunk_embeddings)):
                chunk_vector = np.array(emb)
                collection.add(
                    ids=[f"{doc_id}_chunk{idx}"],
                    documents=[chunk],
                    embeddings=[chunk_vector],
                    metadatas=[{"version": version, "parent_doc": doc_id}]
                )

            logging.info(f"Stored {len(text_chunks)} chunks for DocNo {doc_no}, version {version}")

            # Remove processed file
            os.remove(file_path)

    except Exception as e:
        logging.error(f"Failed processing DocNo {doc_no}: {e}", exc_info=True)

# ----------------------------
# Tenant Processing
# ----------------------------
def process_tenant(tenant_config, db_dir="db/docs", vectordb_dir="db/vectordb"):
    tenant_name = tenant_config.get('Tenant', 'defaulttenant')
    logging.info(f"Processing tenant: {tenant_name}")

    auth_token = utils.basic_auth_token(
        tenant_config['Username'],
        tenant_config['Password']
    )

    db_path = os.path.join(db_dir, f"{tenant_name}.db")
    if not os.path.exists(db_path):
        logging.warning(f"Database not found for tenant '{tenant_name}': {db_path}")
        return

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
        logging.info(f"Processing document {idx}/{total_docs} (DocNo: {doc_no}, Version: {version})")
        process_document(collection, tenant_config, auth_token, doc_no, version)

# ----------------------------
# Main Entry
# ----------------------------
def process(config_path='config/config.json', tenant_name=None, db_dir="db/docs", vectordb_dir="db/vectordb"):
    config = load_config(config_path)
    tenant_configs = get_tenant_configs(config, tenant_name)

    for tenant_config in tenant_configs:
        process_tenant(tenant_config, db_dir=db_dir, vectordb_dir=vectordb_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and embed Therefore documents.")
    parser.add_argument("--tenant", dest="tenant_name", help="Name of the tenant to process")
    parser.add_argument("--config", dest="config_path", default="config/config.json", help="Path to config file")
    parser.add_argument("--db-dir", dest="db_dir", default="db/docs", help="Directory containing tenant SQLite DBs")
    parser.add_argument("--vectordb-dir", dest="vectordb_dir", default="db/vectordb", help="Directory for Chroma persistent DBs")
    args = parser.parse_args()

    process(
        config_path=args.config_path,
        tenant_name=args.tenant_name,
        db_dir=args.db_dir,
        vectordb_dir=args.vectordb_dir
    )
