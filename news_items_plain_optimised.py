import os
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import xml.etree.ElementTree as ET
import json
import pandas as pd

DIGITS_ONLY = re.compile(r"^\d+$")

# --------- Low-level, hot-path helpers ---------

def read_text_fast(p: Path) -> str:
    # Large buffer; avoids many syscalls on spinning disks / network FS
    with p.open("r", encoding="utf-8", buffering=1 << 20) as f:
        return f.read()

def parse_metadata_xml(xml_path: Path) -> dict:
    # Avoid .read()+fromstring; parse directly from file (less Python-level churn)
    tree = ET.parse(xml_path)
    root = tree.getroot()

    pub = root.find("publication")
    issue = pub.find("issue")
    item = issue.find("item")

    # Avoid repeated XPath strings; ElementTree is C-accelerated for .find/.findtext
    return {
        "publication_id": pub.attrib.get("id"),
        "news_source": (pub.findtext("source") or ""),
        "news_title": (pub.findtext("title") or ""),
        "issue_location": (pub.findtext("location") or ""),
        "issue_id": issue.attrib.get("id"),
        "issue_date": (issue.findtext("date") or ""),
        "item_id": item.attrib.get("id"),
        "item_plain_text_file": (item.findtext("plain_text_file") or ""),
        "item_title": (item.findtext("title") or ""),
        "item_type": (item.findtext("item_type") or ""),
        "item_word_count": int(item.findtext("word_count") or 0),
        "item_ocr_quality_mean": (item.findtext("ocr_quality_mean") or ""),
        "item_ocr_quality_sd": (item.findtext("ocr_quality_sd") or ""),
    }

def get_items_from_issue_dir(metadata_issue_dir: Path, plaintext_issue_dir: Path) -> list[dict]:
    """Process one issue: many XML files + matching plaintext files."""
    items = []
    # Use scandir via Path.iterdir() for cheaper stat calls
    for entry in metadata_issue_dir.iterdir():
        if not entry.name.endswith("_metadata.xml") or not entry.is_file():
            continue

        prefix = entry.name[: entry.name.rfind("_metadata.xml")]
        try:
            meta = parse_metadata_xml(entry)
        except Exception as e:
            # Skip bad/partial files but keep going
            # (You can log if you want: print(f"Bad XML {entry}: {e}"))
            continue

        txt_path = plaintext_issue_dir / f"{prefix}.txt"
        try:
            text = read_text_fast(txt_path)
        except FileNotFoundError:
            text = ""  # or continue

        meta["item_text"] = text
        items.append(meta)
    return items

# --------- Mid-level traversal ---------

def get_volume_news_items(volume_path: Path, max_workers: int | None = None) -> list[dict]:
    print(f"  Volume: {volume_path.name}")
    if not volume_path.exists():
        raise FileNotFoundError(volume_path)
    if not volume_path.is_dir():
        raise NotADirectoryError(volume_path)

    # Find metadata/plaintext subdirs once
    metadata_dir = None
    plaintext_dir = None
    for sub in volume_path.iterdir():
        n = sub.name
        if n.endswith("metadata"):
            metadata_dir = sub
        elif n.endswith("plaintext"):
            plaintext_dir = sub

    if metadata_dir is None or plaintext_dir is None:
        raise FileNotFoundError(f"No metadata or plaintext under {volume_path}")

    # Issue folders = /.../metadata/<digits> ; mirror in plaintext
    issue_dirs = [d for d in metadata_dir.iterdir() if d.is_dir() and DIGITS_ONLY.match(d.name)]
    if not issue_dirs:
        return []

    # Thread pool: I/O-bound workload (many file reads + XML parse in C)
    if max_workers is None:
        # plenty for I/O; keep modest to avoid saturating disk
        max_workers = min(32, (os.cpu_count() or 4) * 5)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for mdir in issue_dirs:
            pdir = plaintext_dir / mdir.name
            futs.append(ex.submit(get_items_from_issue_dir, mdir, pdir))

        for fut in as_completed(futs):
            try:
                results.extend(fut.result())
            except Exception:
                # swallow/continue; optionally log
                pass
    return results


def get_news_items(plaintext_news_root: Path) -> list[dict]:
    items: list[dict] = []
    # volumes = directories under .../<news>/plain_text/
    vols = [vol for vol in plaintext_news_root.iterdir() if vol.is_dir()]
    with ProcessPoolExecutor() as executor:
        for vol, v_items in zip(vols, executor.map(get_volume_news_items, vols)):
            items.extend(v_items)
    return items


def get_all_news_items(dataset_root: Path) -> list[dict]:
    items: list[dict] = []
    # news_name is each top-level dataset name under datasets/
    for news_dir in dataset_root.iterdir():
        if not news_dir.is_dir():
            continue
        print(f"Processing news: {news_dir.name}")
        pt_dir = news_dir / "plain_text"
        if not pt_dir.exists():
            # keep going if some datasets are metadata-only
            continue
        news_items = get_news_items(pt_dir)
        for it in news_items:
            it["news_name"] = news_dir.name
        items.extend(news_items)
    return items


def write_news_items_jsonl_stream(news_dir: Path, out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8", buffering=1 << 20) as out:
        if not news_dir.is_dir():
            return
        print(f"Processing news: {news_dir.name}")
        pt_dir = news_dir / "plain_text"
        if not pt_dir.exists():
            return
        for vol in pt_dir.iterdir():
            if not vol.is_dir():
                continue
            print(f"  Volume: {vol.name}")
            for rec in get_volume_news_items(vol):
                rec["news_name"] = news_dir.name
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")


# --------- Main ---------
if __name__ == "__main__":
    start_time = time.time()
    dataset_path = Path("datasets")
    # Fastest + lowest memory:
    # write_all_items_jsonl_stream(dataset_path, Path("generated_files/news_plain_text.jsonl"))
    # print(f"Finished in {time.time() - start_time} seconds.")
    plain_text_dfs_dir = Path("generated_files/plain_text_dfs2")
    if not plain_text_dfs_dir.exists():
        os.makedirs(plain_text_dfs_dir, exist_ok=True)

    for news_dir in dataset_path.iterdir():
        print(f"Processing news: {news_dir.name}")
        out_path = plain_text_dfs_dir / (news_dir.name + ".json")
        #write_news_items_jsonl_stream(news_dir, out_path)
        pt_dir = news_dir / "plain_text"
        if not pt_dir.exists():
            continue
        items = get_news_items(pt_dir)
        with out_path.open("w", encoding="utf-8", buffering=1 << 20) as out:
            out.write("[")
            for item in items:
                item["news_name"] = news_dir.name
                out.write(json.dumps(item, ensure_ascii=False) + "\n")
            out.write("]")

    print(f"Finished in {time.time() - start_time} seconds.")

    # If you still want a DataFrame + JSON (lines=True), this keeps your original contract:
    # all_items = get_all_news_items(dataset_path)
    # df = pd.DataFrame(all_items)
    # Path("generated_files").mkdir(parents=True, exist_ok=True)
    # df.to_json("generated_files/news_plain_text.json", orient="records", lines=True)









