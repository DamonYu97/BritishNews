import json
from elasticsearch import Elasticsearch
from tqdm import tqdm

# Connect to local Elasticsearch (no auth, no SSL)
es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "bl_news"

# Define index mapping (optional but recommended)
mapping = {
    "mappings": {
        "properties": {
            "type": {"type": "constant_keyword", "value": "publication"},
            "o_ids": {"type": "integer"},
            "e_id": {"type": "integer"},
            "title": {"type": "text"},
            "description": {"type": "text"},
            "start_date": {"type": "date"},
            "end_date": {"type": "date"},
            "embedding": {
                "type": "dense_vector",
                "dims": 768,
                "index": True,
                "similarity": "cosine"
            }
        }
    }
}

# Create index if it doesn't exist
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"Created index '{INDEX_NAME}'.")

# Load data from file
with open("./outputs/sentence-transformers/all-mpnet-base-v2/embedded_publications_1.json", "r") as f:
    data = json.load(f)

# set default value for null values
for publication in data:
    publication["type"] = "publication"
    if publication["title"] is None:
        publication["title"] = ""
    if publication["description"] is None:
        publication["description"] = ""
    if publication["start_date"] is None:
        publication["start_date"] = "1001-01-01"
    if publication["end_date"] is None:
        publication["end_date"] = "3000-01-01"


total = len(data)
count = 0
with tqdm(total=total, desc="Ingestion Progress", unit="step") as pbar:
    for doc in data:
        count += 1
        pbar.update(1)
        es.index(index=INDEX_NAME, id=doc["e_id"], document=doc)
