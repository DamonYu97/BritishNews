import os
import re

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


def get_news_items(volume_path: str):
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

    for issue_folder_name in issues_folder_names:
        metadata_issue_path = os.path.join(metadata_path, issue_folder_name)
        plain_text_issue_path = os.path.join(plain_text_path, issue_folder_name)

        for article_metadata_file in os.listdir(metadata_issue_path):
            article_metadata_path = os.path.join(metadata_issue_path, article_metadata_file)
            prefix_end_index = article_metadata_file.rfind("_metadata.xml")
            article_prefix = article_metadata_file[:prefix_end_index]
            article_metadata = get_metadata_from_xml(article_metadata_path)
            print(article_metadata)

            # get the text for article
            plain_text_article_path = os.path.join(plain_text_issue_path, article_prefix+".txt")
            article_text =  open(plain_text_article_path, "r", encoding="utf-8").read()
            article = article_metadata
            article['text'] = article_text
            articles.append(article)
    return articles



def get_metadata_from_xml(article_metadata_path):
    xml_str = open(article_metadata_path).read()
    root = ET.fromstring(xml_str)
    # Extract publication info
    pub = root.find("publication")
    issue = pub.find("issue")
    item = issue.find("item")
    item_info = {
        "id": pub.attrib.get("id"),
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



if __name__ == "__main__":
    volume_path = "/Users/lilinyu/Documents/BritishNews/newspapers/IrvineExpress/BLNewspapers_IrvineExpress_0003086_1884"
    articles = get_news_items(volume_path)
    articles_df = pd.DataFrame(articles)
    articles_df.to_json("BLNewspapers_IrvineExpress_0003086_1884.json", orient="records", lines=True)






