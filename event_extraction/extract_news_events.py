import json
import sys

from tqdm import tqdm

from omni.extractor import Extractor
from utils import get_google_cloud_storage, chunk

if __name__ == '__main__':
    # load dataframe
    #batch_size = int(sys.argv[2])
    schema = sys.argv[2]
    cloud_storage = get_google_cloud_storage()
    input_filename = f"ee/sources/{sys.argv[1]}"
    articles = cloud_storage.read_json(input_filename)
    articles = articles
    print(f"{len(articles)} news articles loaded!")
    extractor = Extractor()
    for article in tqdm(articles):
        # split article into small chunks with at most 20 tokens, each chunk should include only complete sentences.
        article_chunks, offsets = chunk(article["item_text"], max_sequence_length=20)
        chunk_ee_tmp = extractor.extract_events(article_chunks, schema)
        article_ee_result = []
        for offset, c_ee in zip(offsets, chunk_ee_tmp):
            if len(c_ee["events"]) > 0:
                article_ee_result.append({
                    "offset": offset,
                    "events": c_ee["events"],
                })
        article["ee_result"] = article_ee_result
        print(article_ee_result)

    news_name = articles[0]["news_name"]
    output_filename = f"ee/results/{news_name}_ee_{schema}_test.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(articles), output_filename)