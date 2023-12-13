import boto3
import json
from decimal import Decimal
import time

aws_access_key_id = ""
aws_secret_access_key = ""
region_name = "us-west-1"

dynamodb = boto3.resource(
    "dynamodb",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


s3_client = boto3.client(
    "s3",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

bucket_name = "awsminitaskbucket"
# ===================================================================================================

def create_table():
    dynamodb_client = boto3.client(
        "dynamodb",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    dynamodb_client.create_table(
        TableName="Books",
        KeySchema=[
            {"AttributeName": "year", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "year", "AttributeType": "N"},
            {"AttributeName": "title", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    print("Tabela została utworzona")


try:
    create_table()
except Exception as e:
    print(f"Nie udało się utworzyć tabeli: {e}")

time.sleep(10)


def add_book(table_name, year, title):
    table = dynamodb.Table(table_name)
    try:
        response = table.put_item(
            Item={
                "year": int(year),
                "title": title,
            }
        )
        print(f"Dodano książkę: {title} z roku {year} do tabeli '{table_name}'")
    except Exception as e:
        print(f"Nie udało się dodać książki do tabeli: {e}")


add_book("Books", 2023, "Example Book 1")
add_book("Books", 2022, "Example Book 2")
add_book("Books", 2021, "Example Book 3")


def get_table_items(table_name):
    table = dynamodb.Table(table_name)
    response = table.scan()

    print("\nLista książek z tabeli 'Books':")
    for item in response["Items"]:
        print(item)

    return response["Items"]


get_table_items("Books")


# ===================================================================================================
def create_bucket(bucket_name):
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region_name},
        )
        return True
    except Exception as e:
        print(f"Nie udało się utworzyć koszyka: {e}")
        return False


create_bucket("awsminitaskbucket")
time.sleep(5)

print(f"Bucket '{bucket_name}' został utworzony.")

s3 = boto3.resource(
    "s3",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
time.sleep(5)


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")


def generate_report(table_name):
    table = dynamodb.Table(table_name)
    items = table.scan()["Items"]
    report_data = {}

    for idx, item in enumerate(items, start=1):
        report_data[f"book_{idx}"] = {"title": item["title"], "year": item["year"]}

    return json.dumps(report_data, default=decimal_default)


def upload_report_to_s3(report_data, bucket_name):
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.put_object(Key="report.json", Body=report_data)
        print(f"Raport został zapisany w koszyku '{bucket_name}'")
        return True
    except Exception as e:
        print(f"Nie udało się zapisać raportu w koszyku: {e}")
        return False


report_data = generate_report("Books")
upload_report_to_s3(report_data, "awsminitaskbucket")
time.sleep(5)


def get_s3_report_content(file_name):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        print("Zawartość pliku JSON:")
        print(data)
        return True
    except Exception as e:
        print(f"Błąd podczas pobierania danych: {e}")
        return False


get_s3_report_content("report.json")
