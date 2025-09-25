import json

import requests
from bs4 import BeautifulSoup
import re

base_url = 'https://bl.iro.bl.uk/'


def fetch_all_newspapers_metadata(url):
    page_num = 1
    total_pages = 14
    url_for_page = url + "&page=" + str(page_num)
    news_list = []
    while page_num <= total_pages:
        print("Fetching page " + str(page_num) + " of " + str(total_pages))
        response = requests.get(url_for_page, timeout=60)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the select tag with id "page_url"
            tbody_tag = soup.find('tbody')

            # Check if the select tag was found
            if tbody_tag:
                news_tr_list = tbody_tag.find_all('tr', recursive=False)
                for news_tr in news_tr_list:
                    row_id = news_tr.attrs['id']
                    title_tag = news_tr.find(class_='search-result-title').find('a')
                    title = title_tag.text
                    news_dataset_url = base_url + title_tag.attrs['href']
                    creator_tags = news_tr.find_all('a', class_='creator-search-results')
                    # remove the first empty creator tag
                    creator_tags = creator_tags[1:]
                    creators = [re.search(r'\w+(\s\w+)*', tag.text).group() for tag in creator_tags]
                    print(f"Fetching newspaper for: {title}")
                    metadata = fetch_single_newspaper_metadata(news_dataset_url)
                    metadata["creators"] = creators
                    metadata["title"] = title
                    metadata["url"] = news_dataset_url
                    news_list.append(metadata)

            else:
                print("Newspaper list is empty")
        else:
            print(f"Failed to fetch the page {page_num}. Status code:", response.status_code)

        page_num += 1
        url_for_page = url + "&page=" + str(page_num)
    return news_list


def fetch_single_newspaper_metadata(url):
    first_page_res = requests.get(url, timeout=60)
    metadata = {}
    if first_page_res.status_code == 200:
        soup = BeautifulSoup(first_page_res.text, 'html.parser')
        abstract = soup.find('li', class_='attribute-abstract').text
        date_published = soup.find('li', class_='attribute-date_published').text
        institution = soup.find('li', class_='attribute-institution').text
        project_name = soup.find('li', class_='attribute-project_name').text
        #funder_tag = soup.find('div', class_='funder')
        #funder = {}
        publisher = soup.find('li', class_='attribute-publisher').text
        place_of_publication = soup.find('li', class_='attribute-place_of_publication').text
        official_url = soup.find('li', class_='attribute-official_link').text
        licence = soup.find('li', class_='attribute-license').text
        doi = soup.find('li', class_='attribute-doi').text
        additional_info = soup.find('li', class_='attribute-add_info').text

        # fetch file items
        items = get_files_metadata_for_single_newspaper(url, soup)
        years_available = [item['year'] for item in items]

        metadata = {
            'abstract': abstract,
            'date_published': date_published,
            'institution': institution,
            'project_name': project_name,
            'publisher': publisher,
            'place_of_publication': place_of_publication,
            'official_url': official_url,
            'licence': licence,
            'doi': doi,
            'additional_info': additional_info,
            'items': items,
            'earliest_year_available': min(years_available) if len(years_available) > 0 else -1,
            'latest_year_available': max(years_available) if len(years_available) > 0 else -1,
        }
    else:
        print(f"Failed to fetch the page {url}. Status code:", first_page_res.status_code)
    return metadata


def get_files_metadata_for_single_newspaper(f_url, f_soup):
    items = []
    soup = f_soup
    page_count = 1
    pagination_tag = soup.find('ul', class_='pagination')
    if pagination_tag:
        page_count = len(pagination_tag.find_all('li', recursive=False)) - 2
    page_num = 1
    while page_num <= page_count:
        t_body_tag = soup.find('tbody')
        if not t_body_tag:
            break
        item_list = t_body_tag.find_all('tr', recursive=False)
        for item in item_list:
            filename_tag = item.find('td', class_='attribute-filename').find('a', recursive=False)
            filename = filename_tag.text
            year_match = re.search(r'_(?P<year>(\d{4}))[._]', filename)
            if not year_match:
                # this is not core file, such as readme, so we skip
                continue
            year = year_match.group('year')
            print(year)

            download_link = base_url + filename_tag['href']
            date_uploaded = item.find('td', class_='attribute-date_uploaded').text
            permission = item.find('td', class_='permission').find('span', recursive=False).text
            file_size = item.find('td', class_='attribute-file_size').text
            file_size = file_size.strip()
            file_size_part = file_size.split()
            file_size_num = 0
            file_size_unit = "MB"
            if len(file_size_part) == 2:
                file_size_num = file_size_part[0]
                file_size_unit = file_size_part[1]
            items.append({
                'filename': filename,
                'year': year,
                'download_link': download_link,
                'date_uploaded': date_uploaded,
                'permission': permission,
                'file_size': file_size_num,
                'file_size_unit': file_size_unit
            })

        # request next page
        page_num += 1
        if page_num > page_count:
            break
        page_url = f_url + "&page=" + str(page_num)
        page_res = requests.get(page_url, timeout=60)
        if page_res.status_code == 200:
            soup = BeautifulSoup(page_res.text, 'html.parser')
        else:
            print(f"Failed to fetch the page {page_num} with this link {page_url}. Status code:", page_res.status_code)
            break
    return items



if __name__ == "__main__":
    newspaper_datasets_url = "https://bl.iro.bl.uk/collections/9a6a4cdd-2bfe-47bb-8c14-c0a5d100501f?locale=en"
    all_news = fetch_all_newspapers_metadata(newspaper_datasets_url)
    with open("generated_files/newspapers_list.json", "w") as f:
        json.dump(all_news, f)