import json
import sys

from tqdm import tqdm

from omni.extractor import Extractor
from utils import get_google_cloud_storage

if __name__ == '__main__':
    # load dataframe
    batch_size = int(sys.argv[1])
    cloud_storage = get_google_cloud_storage()
    input_filename = "ee/sources/broadsides_subs_kg_hq_df"
    df = cloud_storage.read_pandas_json(input_filename, orient="index")
    print(f"{len(df)} broadsides loaded!")
    extractor = Extractor()
    texts = df["description"].values.tolist()
    current_index = 0
    len_texts = len(texts)
    results = []
    with tqdm(total=len_texts, desc="Batch event extraction progress") as pbar:
        while current_index < len_texts:
            if current_index + batch_size > len_texts:
                chunk_result = extractor.extract_events(texts[current_index:])
                pbar.update(len_texts - current_index)
            else:
                chunk_result = extractor.extract_events(texts[current_index:current_index + batch_size])
                pbar.update(batch_size)
            results.extend(chunk_result)
            current_index += batch_size

    output_filename = f"ee/results/broadsides_ee.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(results), output_filename)