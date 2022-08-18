import boto3, csv

AWS_REGION = "us-east-2"
s3 = boto3.resource("s3")
bucketName = "air-pollution-streaming-data"
buckets = []
streamName = "air-pollution-stream"

for bucket in s3.buckets.all():
	buckets.append(bucket.name)

print(f"Buckets: {buckets}")

if bucketName not in buckets:
        location = {"LocationConstraint": AWS_REGION}
        newBucket = s3.create_bucket(Bucket = bucketName, CreateBucketConfiguration = location)
        print(f"AWS Bucket {newBucket} has been created")


firehose = boto3.client("firehose")

try:
	streamResponse = firehose.describe_delivery_stream(DeliveryStreamName = streamName)["DeliveryStreamDescription"]["DeliveryStreamStatus"]
except:
	newStream = firehose.create_delivery_stream(DeliveryStreamName = streamName,
					DeliveryStreamType = "DirectPut",
					S3DestinationConfiguration = {
						"RoleARN": "arn:aws:iam::864212374478:role/firehoseDeliveryRole",
						"BucketARN": f"arn:aws:s3:::{bucketName}"})

streams = firehose.list_delivery_streams()
print(f"Streams: {streams['DeliveryStreamNames']}")

file = open("/home/javierGr/air-pollution-aws-data-pipeline/AirQuality.csv")
data = csv.reader(file)
header = []
header = next(data)

for row in data:
	payload = ",".join(str(value) for value in row)
	print(payload)
	firehose.put_record(DeliveryStreamName = streamName, Record = {"Data": payload + "\n"})

file.close()
