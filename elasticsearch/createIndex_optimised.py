import json
from pathlib import Path
from typing import Iterable, Dict

from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

ELASTIC_HOTS = "https://localhost:9200"
CA_CERT = "cert.crt"
API_KEY = "RW1QX3VKa0JELWcwNGNzS1Q5SEQ6eTU0Q04yaHlSZEM5d1ROY1FHMlFiZw=="

client = Elasticsearch(
    ELASTIC_HOTS,
    ca_certs=CA_CERT,
    api_key=API_KEY
)

INDEX = "bl_news"

settings = {
    "analysis": {
            "analyzer": {
                "default": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "kstem",
                        "stop"
                    ]
                },
                "default_search": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "kstem",
                        "stop"
                        # synonym_graph
                    ]
                }
            }
        }
}

mappings = {
    "properties": {
        "collection": {"type": "constant_keyword", "value": "British Library Newspapers"},
        "publication_id": {"type": "keyword"},
        "news_source": {"type": "keyword"},
        "news_title": {"type": "keyword"},
        "news_name": {"type": "keyword"},
        "issue_location": {"type": "keyword"},
        "issue_id": {"type": "keyword" },
        "issue_date": {"type": "date"},
        "item_id": {"type": "keyword"},
        "item_plain_text_file": {"type": "keyword"},
        "item_title": {"type": "text"},
        "item_type": {"type": "keyword"},
        "item_word_count": {"type": "integer"},
        "item_ocr_quality_mean": {"type": "float"},
        "item_ocr_quality_sd": {"type": "float"},
        "item_text": {"type": "text"}
    }
}

def actions_from_list(docs: Iterable[Dict]) -> Iterable[Dict]:
    for doc in docs:
        doc["collection"] = "British Library Newspapers"
        doc_id = doc["publication_id"] + doc["issue_id"] + doc["item_id"]
        yield {
            "_index": INDEX,
            "_id": doc_id,
            "_op_type": "index",  # use "create" to fail on dup ids instead
            "_source": doc,
        }


def index_file(file_path: Path, thread_count: int = 6, chunk_size: int = 1500) -> int:
    with file_path.open("r", encoding="utf-8") as f:
        docs = json.load(f)  # expects a JSON array
    total = len(docs)
    success = 0

    with tqdm(total=total, desc=f"Processing {file_path.name}", unit="doc") as pbar:
        for ok, info in helpers.parallel_bulk(
            client,
            actions_from_list(docs),
            thread_count=thread_count,
            chunk_size=chunk_size,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            if ok:
                success += 1
            else:
                print('A document failed:', info)
            pbar.update(1)

    return success


def refresh_quietly():
    try:
        client.indices.refresh(index=INDEX)
    except Exception:
        pass


if __name__ == "__main__":
    # Create the index with the defined mapping
    if not client.indices.exists(index=INDEX):
        client.indices.create(index=INDEX, settings=settings, mappings=mappings)

    news_items_folder_path = Path("../generated_files/plain_text_dfs")
    news_items_paths = [item_path for item_path in news_items_folder_path.glob("*.json")]

    total_docs = 0
    for fp in news_items_paths:
        total_docs += index_file(fp, thread_count=6, chunk_size=100)

    refresh_quietly()
    print(f"Done. Indexed {total_docs} documents into '{INDEX}'.")


