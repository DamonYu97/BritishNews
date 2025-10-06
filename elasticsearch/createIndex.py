import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from elasticsearch import Elasticsearch
from tqdm import tqdm

ELASTIC_HOTS = "https://localhost:9200"
CA_CERT = "cert.crt"
API_KEY = "RW1QX3VKa0JELWcwNGNzS1Q5SEQ6eTU0Q04yaHlSZEM5d1ROY1FHMlFiZw=="

client = Elasticsearch(
    ELASTIC_HOTS,
    ca_certs=CA_CERT,
    api_key=API_KEY
)

index = "bl_news"

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


def create_index_for_news(news_file_path):
    print(f"Creating index for {news_file_path}")
    # Load the data
    with open(news_file_path, 'r') as file:
        news_items_list = json.load(file)
        total = len(news_items_list)
        count = 0
        with tqdm(total=total, desc="Ingestion Progress", unit="step") as pbar:
            for doc in news_items_list:
                count += 1
                pbar.update(1)
                doc["news_title"] = doc.pop("title")
                doc["news_source"] = doc.pop("source")
                doc["issue_id"] = doc.pop("issues_id")
                doc["issue_location"] = doc.pop("location")
                doc["item_text"] = doc.pop("text")
                doc["collection"] = "British Library Newspapers"
                doc_id = doc["publication_id"] + doc["issue_id"] + doc["item_id"]
                client.index(index=index, id=doc_id, document=doc)


if __name__ == "__main__":
    # Create the index with the defined mapping
    if not client.indices.exists(index=index):
        client.indices.create(index=index, settings=settings, mappings=mappings)

    news_items_folder_path = Path("../generated_files/plain_text_dfs2")
    news_items_paths = [item_path for item_path in news_items_folder_path.glob("*.json")]
    with ProcessPoolExecutor() as executor:
        executor.map(create_index_for_news, news_items_paths)




