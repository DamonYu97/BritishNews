import os
from io import BytesIO
from time import time
from shutil import unpack_archive
from tempfile import NamedTemporaryFile
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd


def download_file(url, filepath):
    with urlopen(url) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(filepath)

    # with urlopen(url) as zipresp, NamedTemporaryFile() as tfile:
    #     tfile.write(zipresp.read())
    #     tfile.seek(0)
    #     unpack_archive(tfile.name, filepath, format='zip')


def download_single_dataset_items(dataset, destination_dir):
    # create a folder for this dataset if not exists, items in the dataset will be in
    # `<destination_dir>/<title>/<dataset_type>`.
    news_folder_path = os.path.join(destination_dir, dataset['title'])
    dataset_folder_path = os.path.join(str(news_folder_path), dataset['type'])
    print("Downloading dataset to " + dataset_folder_path)
    os.makedirs(dataset_folder_path, exist_ok=True)

    items = dataset["items"]
    for item in items:
        filename = item["filename"][:-4]
        download_url = item["download_link"]
        download_path = os.path.join(dataset_folder_path, filename)
        print(f"Downloading {download_url} to {download_path}")
        download_file(download_url, download_path)


def download_single_news_items(dataset_df, destination_dir, news_title, dataset_type="all"):
    # find news datasets
    condition = (dataset_df["title"] == news_title)
    if dataset_type == "plain_text" or dataset_type == "alto":
        condition &= (dataset_df["type"] == dataset_type)
    # if not plai_text or alto, it will download for both types
    news_datasets = dataset_df[condition].to_dict(orient="records")
    for news_dataset in news_datasets:
        download_single_dataset_items(news_dataset, destination_dir)


def main():
    # load all news metadata, which has news title and file download links
    news_scraped_metadata_df = pd.read_json("generated_files/newspapers_list.json")
    # reformat title and get dataset type
    news_scraped_metadata_df['type'] = news_scraped_metadata_df['title'].apply(
        lambda x: 'plain_text' if x.endswith('[plaintext]') or x.endswith('(plaintext)') else 'alto')
    news_scraped_metadata_df['title'] = news_scraped_metadata_df["title"].str.replace(r"\s*[\[\(]plaintext[\]\)]$", "",
                                                                                      regex=True)
    test_news_title = "Widnes Examiner"
    test_destination = "/Users/lilinyu/Documents/PhD/BritishNews/"
    download_single_news_items(news_scraped_metadata_df, test_destination, test_news_title, "plain_text")

if __name__ == "__main__":
    # test_url = "https://bl.iro.bl.uk//downloads/3dd95669-433f-46e1-ac95-0a8543951de9?locale=en"
    # test_filepath = "/Users/lilinyu/Documents/PhD/BritishNews/BLNewspapers_WidnesExaminer_0002601_1876"
    # start_time = time()
    # download_file(test_url, test_filepath)
    # print("Download time: " + str(time() - start_time))
    main()









