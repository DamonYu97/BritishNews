import json
import sys

from tqdm import tqdm

from omni.extractor import Extractor
from utils import get_google_cloud_storage

if __name__ == '__main__':
    # load dataframe
    batch_size = int(sys.argv[2])
    schema = sys.argv[3]
    cloud_storage = get_google_cloud_storage()
    input_filename = f"ee/sources/{sys.argv[1]}"
    articles = cloud_storage.read_json(input_filename)
    print(f"{len(articles)} news articles loaded!")
    texts = [article["item_text"] for article in articles]
    extractor = Extractor()
    current_index = 0
    len_texts = len(articles)
    results = []
    with tqdm(total=len_texts, desc="Batch event extraction progress") as pbar:
        while current_index < len_texts:
            if current_index + batch_size > len_texts:
                chunk_result = extractor.extract_events(texts[current_index:], schema)
                pbar.update(len_texts - current_index)
            else:
                chunk_result = extractor.extract_events(texts[current_index:current_index + batch_size], schema)
                pbar.update(batch_size)
            results.extend(chunk_result)
            current_index += batch_size
    news_name = articles[0]["news_name"]
    output_filename = f"ee/results/{news_name}_ee_{schema}.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(results), output_filename)