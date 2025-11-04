import json
from omni.extractor import Extractor
from utils import get_google_cloud_storage

if __name__ == '__main__':
    # load dataframe
    cloud_storage = get_google_cloud_storage()
    input_filename = "ee/sources/broadsides_subs_kg_hq_df"
    df = cloud_storage.read_pandas_json(input_filename, orient="index")
    print(f"{len(df)} broadsides loaded!")
    extractor = Extractor()
    texts = df["description"].values.tolist()
    results = extractor.extract_events(texts)
    output_filename = "ee/results/broadsides_ee.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(results), output_filename)