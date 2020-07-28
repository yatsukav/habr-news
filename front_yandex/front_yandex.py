import json
import boto3

def handler(event, context):
    # make connection to s3
    session = boto3.session.Session()
    s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

    # get data
    get_object_response = s3.get_object(Bucket="scrapper", Key="habr/news")
    response_body = get_object_response["Body"].read()

    # parse data
    response_parsed = response_body.decode("utf-8").split("\n")

    # limit data for response
    size = 0
    count = 0
    text_body = []
    for header in response_parsed:
        count += 1
        size += len(header) + len("\n\n")
        if size > 1024:
            break
        if count > 6:
            break
        text_body.append(header)

    # return successul result
    return json.dumps({
        "response": {"text": "\n\n".join(text_body), "end_session": True},
        "session": event["session"],
        "version": event["version"],
    })
