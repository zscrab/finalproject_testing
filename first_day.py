
import boto3
import pandas as pd
import json

s3_client = boto3.client('s3')
bucket_list = s3_client.list_buckets()
bucket_name = 'data-28-final-project-files-group2'
bucket_contents = s3_client.list_objects_v2(Bucket='data-28-final-project-files-group2', Prefix='Talent')["Contents"]


def s3_json_scrape(path):
    """generator to iterate over the s3 bucket"""
    json_results = json.loads(path)
    for i in json_results:
        yield i


def read_json(filename: str) -> dict:
    try:
        with open(filename, "r") as f:
            data = json.loads(f.read())
    except:
        raise Exception(f"Reading {filename} file encountered an error")
    return data

read_json(bucket_contents)

# def convert_to_csv(file):
#     data = pd.DataFrame.from_dict(file, orient='index').T
#     #data.to_csv(f"{file}.csv", encoding='utf-8', index=False)
#     data.to_csv(f"{file_name}.csv", encoding='utf-8', index=False)

# def s3_json_reader(json_file):
#     read_json_file = s3_client.get_object(Bucket=bucket_name, Key=json_file)
#     return pd.read_json(read_json_file.get("Body"))
#
#
# def number_dos():
#     for result in s3_json_scrape(bucket_contents):
#        print(result)
#
# number_dos()
#
# # frame = pd.DataFrame()
# # for filename in bucket_contents:
# #     result = read_json(filename)
# #     tmp_frame = pd.read_json(result)
# #     frame = frame.append(tmp_frame, ignore_index=True)
# #
# # frame.to_csv('output.csv', index=False)