from functions import functions
import pytest
import boto3

dynamo_db = boto3.client(
    "dynamodb",
    region_name=functions.region_name,
    aws_access_key_id=functions.aws_access_key_id,
    aws_secret_access_key=functions.aws_secret_access_key,
)

s3 = boto3.client(
    "s3",
    region_name=functions.region_name,
    aws_access_key_id=functions.aws_access_key_id,
    aws_secret_access_key=functions.aws_secret_access_key,
)


def test_create_table():
    response = dynamo_db.describe_table(TableName="Books")
    assert response["Table"]["TableName"] == "Books"
    assert len(response["Table"]["KeySchema"]) == 2
    assert response["Table"]["KeySchema"][0]["AttributeName"] == "year"
    assert response["Table"]["KeySchema"][0]["KeyType"] == "HASH"
    assert response["Table"]["KeySchema"][1]["AttributeName"] == "title"
    assert response["Table"]["KeySchema"][1]["KeyType"] == "RANGE"
    assert len(response["Table"]["AttributeDefinitions"]) == 2
    assert {"AttributeName": "year", "AttributeType": "N"} in response["Table"][
        "AttributeDefinitions"
    ]
    assert {"AttributeName": "title", "AttributeType": "S"} in response["Table"][
        "AttributeDefinitions"
    ]
    assert response["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"] == 10
    assert response["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"] == 10


def test_add_book_success(capsys):
    table_name = "Books"
    year = "2022"
    title = "Test Book"
    functions.add_book(table_name, year, title)
    captured = capsys.readouterr()
    assert (
        f"Dodano książkę: {title} z roku {year} do tabeli '{table_name}'"
        in captured.out
    )


def test_add_book_exception(capsys):
    table_name = "Bookss"
    year = "2022"
    title = "Test Book"
    functions.add_book(table_name, year, title)
    captured = capsys.readouterr()
    assert f"Nie udało się dodać książki do tabeli:" in captured.out


def test_get_table_items(capsys):
    table_name = "Books"
    items = functions.get_table_items(table_name)
    captured = capsys.readouterr()

    assert {"year": 2023, "title": "Example Book 1"} in items
    assert {"year": 2022, "title": "Example Book 2"} in items
    assert {"year": 2021, "title": "Example Book 3"} in items

    assert "Lista książek z tabeli 'Books':" in captured.out
    assert "{'year': Decimal('2023'), 'title': 'Example Book 1'}" in captured.out
    assert "{'year': Decimal('2022'), 'title': 'Example Book 2'}" in captured.out
    assert "{'year': Decimal('2021'), 'title': 'Example Book 3'}" in captured.out


@pytest.mark.parametrize(
    "bucket_name",
    [
        "awsminitaskbucket",
    ],
)
def test_create_bucket_success(bucket_name):
    response = s3.list_buckets()
    assert bucket_name in str(response)


@pytest.mark.parametrize(
    "bucket_name",
    [
        "fakebucket",
    ],
)
def test_create_bucket_failure(bucket_name):
    response = s3.list_buckets()
    assert bucket_name not in response


@pytest.mark.parametrize(
    "bucket_name, file_name",
    [
        ("awsminitaskbucket", "report.json"),
    ],
)
def test_upload_report_to_s3(bucket_name, file_name):
    objects = s3.list_objects(Bucket=bucket_name)

    assert file_name in objects["Contents"][0]["Key"]
