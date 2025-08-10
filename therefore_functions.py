import json
import http.client
import os
import uuid

def _get_therefore_converted_document(base_url, tenant, auth_token, doc_no, version=0):
    conn = http.client.HTTPSConnection(base_url.replace('https://', ''))
    payload = json.dumps({
    "ConversionOptions": {
        "AnnotationMode": 0,
        "ConvertTo": 5,
        "SignatureMode": 0
    },
    "DocNo": doc_no,
    "StreamNos": [],
    "VersionNo": version
    })
    headers = {
        'TenantName': tenant,
        'Content-Type': 'application/json',
        'Authorization': auth_token
    }
    conn.request("POST", "/theservice/v0001/restun/GetConvertedDocStreams", payload, headers)
    res = conn.getresponse()
    if res.status != 200:
        raise Exception(f"Failed to fetch converted document: {res.status} {res.reason}")
    data = res.read()
    response_json = json.loads(data.decode("utf-8"))
    return  response_json

def _save_therefore_converted_document(converted_document, output_dir):
    saved_files = []
    streams = converted_document.get("Streams", [])
    for stream in streams:
        file_data = stream.get("FileData")
        file_name = stream.get("FileName", "output.pdf")
        file_path = os.path.join(output_dir, file_name)
        if os.path.exists(file_path):
            unique_id = uuid.uuid4().hex[:8]  # Generate a unique ID
            file_name = f"{os.path.splitext(file_name)[0]}_{unique_id}{os.path.splitext(file_name)[1]}"
        if file_data:
            file_bytes = bytes(file_data)
            file_path = os.path.join(output_dir, file_name)
            with open(file_path, "wb") as f:
                f.write(file_bytes)
            print(f"Saved: {file_path}")
            saved_files.append(file_path)
    return saved_files

def convert_and_save_document(base_url, tenant, auth_token, doc_no, output_dir, version=0):
    try:
        converted_document = _get_therefore_converted_document(base_url, tenant, auth_token, doc_no, version)
        return _save_therefore_converted_document(converted_document, output_dir)
    except Exception as e:
        print(f"Failed to convert and save document {doc_no}: {e}")

def query_all_category_documents(base_url, tenant, auth_token, category_no, max_rows=5000000, row_block_size=500):
    conn = http.client.HTTPSConnection(base_url.replace('https://', ''))
    payload = json.dumps({
        "Query": {
            "CategoryNo": category_no,
            "MaxRows": max_rows,
            "OrderByFieldsNoOrNames": [],
            "RowBlockSize": row_block_size
        }
    })
    headers = {
        'TenantName': tenant,
        'Content-Type': 'application/json',
        'Authorization': auth_token
    }
    # Initial query
    conn.request("POST", "/theservice/v0001/restun/ExecuteAsyncSingleQuery", payload, headers)
    res = conn.getresponse()
    data = res.read()
    response = json.loads(data.decode("utf-8"))

    query_id = response["QueryId"]
    all_rows = response["QueryResult"]["ResultRows"]
    has_remaining = response.get("HasRemainingRows", False)

    # Fetch additional rows if needed
    while has_remaining:
        next_payload = json.dumps({"QueryID": query_id, "RowBlockSize": row_block_size})
        conn.request("POST", "/theservice/v0001/restun/GetNextSingleQueryRows", next_payload, headers)
        res = conn.getresponse()
        data = res.read()
        next_response = json.loads(data.decode("utf-8"))
        all_rows.extend(next_response["QueryResult"]["ResultRows"])
        has_remaining = next_response.get("HasRemainingRows", False)

    # Release the query
    release_payload = json.dumps({"QueryID": query_id})
    conn.request("POST", "/theservice/v0001/restun/ReleaseSingleQuery", release_payload, headers)
    conn.getresponse().read()  # Consume response

    return all_rows


def _get_items_of_type(node, results, type = 2):
    if isinstance(node, dict):
        if node.get('ItemType') == type:
            results.append({'ItemNo': node.get('ItemNo'), 'Name': node.get('Name')})
        # Recursively check for child items
        for key in node:
            if isinstance(node[key], list):
                for item in node[key]:
                    _get_items_of_type(item, results, type)
    elif isinstance(node, list):
        for item in node:
            _get_items_of_type(item, results, type)

def get_all_categories(base_url, tenant, auth_token):
    # Create connection and request
    conn = http.client.HTTPSConnection(base_url.replace('https://', ''))
    payload = json.dumps({})
    headers = {
        'TenantName': tenant,
        'Content-Type': 'application/json',
        'Authorization': auth_token
    }
    conn.request("POST", "/theservice/v0001/restun/GetCategoriesTree", payload, headers)
    res = conn.getresponse()
    data = res.read()
    response_json = json.loads(data.decode("utf-8"))
    category_list = []
    _get_items_of_type(response_json.get('TreeItems', []), category_list, type=2)
    return category_list