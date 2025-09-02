import json
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")

def get_owner(bucket):
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket)
        for t in tags.get("TagSet", []):
            if t["Key"].lower() == "owner":
                return t["Value"]
    except:
        return "—"
    return "—"

def lambda_handler(event, context):
    resp = s3.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_owner, n): n for n in names}
        for f in as_completed(futures):
            results.append({"name": futures[f], "owner": f.result()})
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({"buckets": results})
    }

