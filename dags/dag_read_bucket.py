import csv
import boto3

with open('access_keys_aws/luisftakahashi_1_accessKeys.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        access_key_id = list(row.values())[0]
        secret_access_key = list(row.values())[1]


session = boto3.Session( 
         aws_access_key_id=access_key_id, 
         aws_secret_access_key=secret_access_key)

#Then use the session to get the resource
s3 = session.resource('s3')

my_bucket = s3.Bucket('datalake-xpe-raw')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)