import json
import os
import boto3
from botocore.exceptions import ClientError

# Globalni DynamoDB resource (u pravoj Lambdi ovo je ok zbog reuse konekcije)
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("ORDERS_TABLE", "Orders")


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    # 1) Parsiranje JSON-a
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"message": "Invalid JSON"})

    # 2) Validacija polja
    order_id = body.get("order_id")
    amount = body.get("amount")

    if not order_id or amount is None:
        return _response(400, {"message": "order_id and amount required"})

    # 3) amount u float
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return _response(400, {"message": "amount must be a number"})

    # 4) Upis u DynamoDB
    table = dynamodb.Table(TABLE_NAME)
    try:
        table.put_item(Item={"order_id": order_id, "amount": amount})
    except ClientError:
        return _response(500, {"message": "Failed to save order"})

    # 5) Provera da li je "big order"
    threshold = float(os.environ.get("BIG_ORDER_THRESHOLD", "100"))
    is_big = amount >= threshold

    # 6) HTTP odgovor
    return _response(
        200,
        {
            "order_id": order_id,
            "amount": amount,
            "is_big": is_big,
        },
    )