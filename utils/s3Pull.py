s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIAZCIFP3PAYQQDIKFY',
    aws_secret_access_key='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'
)

for bucket in s3.buckets.all():
    print(bucket.name)

s3.Bucket('cap6640').download_file(Key='2020-02/us-presidential-tweet-id-2020-02-01-00.txt', Filename='us-presidential-tweet-id-2020-02-01-00_local.txt')
pd.read_csv('us-presidential-tweet-id-2020-02-01-00_local.txt', index_col=0, delimiter = "\t")