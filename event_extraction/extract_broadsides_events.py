import json
import sys

from tqdm import tqdm

from omni.extractor import Extractor
from utils import get_google_cloud_storage, chunk

if __name__ == '__main__':
    # load dataframe
    schema = sys.argv[1]
    cloud_storage = get_google_cloud_storage()
    input_filename = "ee/sources/broadsides_subs_kg_hq_df"
    df = cloud_storage.read_pandas_json(input_filename, orient="index")
    print(f"{len(df)} broadsides loaded!")
    extractor = Extractor()
    texts = df["description"].values.tolist()
    current_index = 0
    len_texts = len(texts)
    results = []
    ee_results = []
    for text in tqdm(texts):
        # split article into small chunks with at most 20 tokens, each chunk should include only complete sentences.
        chunks, offsets = chunk(text, max_sequence_length=20)
        chunk_ee_tmp = extractor.batch_extract_events(chunks, schema)
        ee_result = []
        for offset, c_ee in zip(offsets, chunk_ee_tmp):
            if len(c_ee["events"]) > 0:
                ee_result.append({
                    "offset": offset,
                    "events": c_ee["events"],
                })
        ee_results.append(ee_result)
    df["ee_result"] = ee_results
    output_filename = f"ee/results/broadsides_ee_{schema}.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(df.to_json()), output_filename)