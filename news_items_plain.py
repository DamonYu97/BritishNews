import concurrent
import os
import re
import time

import pandas as pd
from pydantic import BaseModel
import xml.etree.ElementTree as ET


class Item(BaseModel):
    title: str
    id: str
    plain_text_file: str
    type: str
    word_count: int
    ocr_quality_mean: float
    ocr_quality_std: float

class Issue(BaseModel):
    id: str
    date: str
    items: list[Item]


class Publication(BaseModel):
    id: str
    title: str
    source: str
    location: str
    issues: list[Issue]


class Newspaper(BaseModel):
    title: str
    publications: list[Publication]


def get_items_from_issues(metadata_issue_path, plain_text_issue_path):
    articles = []
    for article_metadata_file in os.listdir(metadata_issue_path):
        article_metadata_path = os.path.join(metadata_issue_path, article_metadata_file)
        prefix_end_index = article_metadata_file.rfind("_metadata.xml")
        article_prefix = article_metadata_file[:prefix_end_index]
        article_metadata = get_metadata_from_xml(article_metadata_path)
        # print(article_metadata)

        # get the text for article
        plain_text_article_path = os.path.join(plain_text_issue_path, article_prefix + ".txt")
        article_text = open(plain_text_article_path, "r", encoding="utf-8").read()
        article = article_metadata
        article['text'] = article_text
        articles.append(article)
    return articles


def get_volume_news_items(volume_path: str):
    print(f"Processing volume: {volume_path}")
    articles = []
    # check if it is a directory
    if not os.path.exists(volume_path):
        raise FileExistsError

    if not os.path.isdir(volume_path):
        raise NotADirectoryError

    # get metadata and plain text folders path
    metadata_path = None
    plain_text_path = None
    for sub_dir in os.listdir(volume_path):
        if sub_dir.endswith("metadata"):
            metadata_path = os.path.join(volume_path, sub_dir)
        elif sub_dir.endswith("plaintext"):
            plain_text_path = os.path.join(volume_path, sub_dir)

    if metadata_path is None or plain_text_path is None:
        raise FileExistsError("No metadata or plain text files found")
    # list issue folder names
    issues_folder_names = [sub_dir for sub_dir in os.listdir(metadata_path) if re.match(r"^\d+$", sub_dir)]
    metadata_issue_paths = []
    plain_text_issue_paths = []
    for issue_folder_name in issues_folder_names:
        metadata_issue_path = os.path.join(metadata_path, issue_folder_name)
        plain_text_issue_path = os.path.join(plain_text_path, issue_folder_name)
        metadata_issue_paths.append(metadata_issue_path)
        plain_text_issue_paths.append(plain_text_issue_path)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for items in executor.map(get_items_from_issues, metadata_issue_paths, plain_text_issue_paths):
            articles.extend(items)
    return articles



def get_metadata_from_xml(article_metadata_path):
    xml_str = open(article_metadata_path).read()
    root = ET.fromstring(xml_str)
    # Extract publication info
    pub = root.find("publication")
    issue = pub.find("issue")
    item = issue.find("item")
    item_info = {
        "publication_id": pub.attrib.get("id"),
        "source": pub.findtext("source"),
        "title": pub.findtext("title"),
        "location": pub.findtext("location"),
        "issues_id": issue.attrib.get("id"),
        "issue_date": issue.findtext("date"),
        "item_id": item.attrib.get("id"),
        "item_plain_text_file": item.findtext("plain_text_file"),
        "item_title": item.findtext("title"),
        "item_type": item.findtext("item_type"),
        "item_word_count": int(item.findtext("word_count")),
        "item_ocr_quality_mean": item.findtext("ocr_quality_mean"),
        "item_ocr_quality_sd": item.findtext("ocr_quality_sd")
    }

    return item_info


def get_news_items(plaintext_dataset_path):
    volume_names = os.listdir(plaintext_dataset_path)
    items = []
    for volume_name in volume_names:
        print(f"Processing volume: {volume_name}")
        volume_path = os.path.join(plaintext_dataset_path, volume_name)
        items.extend(get_volume_news_items(volume_path))
    return items


def get_all_news_items(dataset_path):
    news_names = os.listdir(dataset_path)
    items = []
    for news_name in news_names:
        print(f"Processing news: {news_name}")
        news_path = os.path.join(dataset_path, news_name)
        news_plain_text_path = os.path.join(news_path, "plain_text")
        items.extend(get_news_items(news_plain_text_path))
    return items


if __name__ == "__main__":
    # test_volume_path = "datasets/Widnes Examiner/plain_text/BLNewspapers_WidnesExaminer_0002601_1876"
    # articles = get_volume_news_items(test_volume_path)
    # print(f"Total time: {time.time() - start}")
    # articles_df = pd.DataFrame(articles)
    # articles_df.to_json("generated_files/BLNewspapers_WidnesExaminer_0002601_1876.json", orient="records", lines=True)
    # news_dataset_path = "datasets/Widnes Examiner/plain_text"
    # all_items = get_news_items(news_dataset_path)
    # all_items_df = pd.DataFrame(all_items)
    # all_items_df.to_json("generated_files/WidnesExaminer_plain_text.json", orient="records", lines=True)
    dataset_path = "datasets"
    all_items = get_all_news_items(dataset_path)
    all_items_df = pd.DataFrame(all_items)
    all_items_df.to_json("generated_files/news_plain_text.json", orient="records", lines=True)








