import pandas as pd
from omni.extractor import Extractor
if __name__ == '__main__':
    # load dataframe
    df = pd.read_json("../generated_files/broadsides_subs_kg_hq_df", orient="index")
    extractor = Extractor()
    texts = df["description"].values.tolist()
    results = extractor.extract_events(texts[:10])
    print(results)