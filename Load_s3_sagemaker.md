
--------Find all file paths in bucket-------------------------
s3 = boto3.client('s3')

response = s3.list_objects_v2(Bucket='sentiment-timmosky', Prefix='Allsentiments/')

for obj in response.get('Contents', []):
    print(obj['Key'])


--------- Load s3 files to Sagemaker notebook-------------------

Bucket = 's3://sentiment-timmosky/Allsentiments/'  
file_key = 'Allsentiments/all_combined.csv'
file_location = 's3://{}/{}/'.format(bucket, file_key)
pd.read_csv(file_location)

df = pd.read_csv(file_location)
----------------------------------------------
https://huggingface.co/distilbert/distilbert-base-uncased-finetuned-sst-2-english
