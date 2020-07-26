import os
import json
import boto3

def handler(event, context):
    # parse event
    query = json.loads(event['body'])

    # make connection to s3
    session = boto3.session.Session(
        aws_access_key_id=os.environ["aws_access_key_id"], 
        aws_secret_access_key=os.environ["aws_secret_access_key"], 
        region_name=os.environ["region_name"]
    )
    s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

    # get data
    get_object_response = s3.get_object(Bucket="scrapper", Key="habr/news")
    response_body = get_object_response["Body"].read()

    # parse data
    response_parsed = response_body.decode("utf-8").split("\n")

    # limit data for response
    size = 0
    text_body = []
    for header in response_parsed:
        size += len(header) + len("\n\n")
        if size > 1024:
            break
        text_body.append(header)

    # return successul result
    return json.dumps({
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": {
            "response": {"text": "\n\n".join(text_body), "end_session": True},
            "session": {
                "session_id": query['session']['session_id'],
                "message_id": query['session']['message_id'],
                "user_id": query['session']['user_id'],
            },
            "version": "1.0",
        },
    })
