import json
import sys

from omni.extractor import Extractor
from utils import get_google_cloud_storage

if __name__ == '__main__':
    # load dataframe
    from_index = int(sys.argv[1])
    to_index = int(sys.argv[2])
    cloud_storage = get_google_cloud_storage()
    input_filename = "ee/sources/broadsides_subs_kg_hq_df"
    df = cloud_storage.read_pandas_json(input_filename, orient="index")
    print(f"{len(df)} broadsides loaded!")
    extractor = Extractor()
    texts = df["description"].values.tolist()
    results = extractor.extract_events(texts[from_index:to_index])
    output_filename = f"ee/results/broadsides_ee_{from_index}_{to_index}.json"
    print(f"Saving results to {output_filename}")
    cloud_storage.write_str(json.dumps(results), output_filename)