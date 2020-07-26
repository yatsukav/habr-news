import os
import unicodedata

import requests
from lxml import html
import boto3

def handler(event, context):
    # fake agent
    user_agent = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36'

    # download page
    url = os.getenv('URL', "https://habr.com/ru/news/")
    page = requests.get(url, headers={"User-Agent": user_agent,})
    content = page.text
    parsed = html.fromstring(content)

    # parse page
    data = []
    headers = parsed.xpath('//h2[@class="post__title"]')
    for header in headers:
        _id = header.xpath('./a[@class="post__title_link"]/@href')[0]
        text = header.xpath('./a[@class="post__title_link"]/text()')[0]
        text = unicodedata.normalize("NFKD", text)
        data.append({"_id": _id, "text": text})

    # make connection to s3
    session = boto3.session.Session(
        aws_access_key_id=os.environ["aws_access_key_id"], 
        aws_secret_access_key=os.environ["aws_secret_access_key"], 
        region_name=os.environ["region_name"]
    )
    s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

    # overwrite data
    storage_data = ".\n".join([x["text"] for x in data])
    s3_response = s3.put_object(Bucket="scrapper", Key="habr/news", Body=storage_data)

    # return successul result
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'isBase64Encoded': False,
        'body': 'Downloaded from {}:\n{}\n\nS3:{}\n'.format(url, storage_data, str(s3_response))
    }
