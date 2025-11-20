import os
os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"

import json
import os
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

import lambda_function


class TestLambdaFunction(unittest.TestCase):
    def setUp(self):
        # Osnovni validan event (API Gateway proxy)
        self.base_event = {
            "body": json.dumps({"order_id": "123", "amount": 50})
        }

    @patch.dict(os.environ, {"BIG_ORDER_THRESHOLD": "100", "ORDERS_TABLE": "Orders"}, clear=True)
    @patch("lambda_function.dynamodb")
    def test_valid_small_order(self, mock_dynamodb):
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        response = lambda_function.lambda_handler(self.base_event, None)

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["order_id"], "123")
        self.assertEqual(body["amount"], 50.0)
        self.assertFalse(body["is_big"])

        mock_table.put_item.assert_called_once_with(
            Item={"order_id": "123", "amount": 50.0}
        )

    @patch.dict(os.environ, {"BIG_ORDER_THRESHOLD": "100", "ORDERS_TABLE": "Orders"}, clear=True)
    @patch("lambda_function.dynamodb")
    def test_valid_big_order(self, mock_dynamodb):
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        event = {"body": json.dumps({"order_id": "999", "amount": 150})}

        response = lambda_function.lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertTrue(body["is_big"])
        mock_table.put_item.assert_called_once()

    @patch.dict(os.environ, {"BIG_ORDER_THRESHOLD": "100", "ORDERS_TABLE": "Orders"}, clear=True)
    @patch("lambda_function.dynamodb")
    def test_missing_fields_returns_400(self, mock_dynamodb):
        event = {"body": json.dumps({"amount": 10})}  # nema order_id

        response = lambda_function.lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 400)
        mock_dynamodb.Table.assert_not_called()

    @patch.dict(os.environ, {"BIG_ORDER_THRESHOLD": "100", "ORDERS_TABLE": "Orders"}, clear=True)
    @patch("lambda_function.dynamodb")
    def test_invalid_json_returns_400(self, mock_dynamodb):
        event = {"body": "{not-json"}  # pokvaren JSON

        response = lambda_function.lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 400)
        mock_dynamodb.Table.assert_not_called()

    @patch.dict(os.environ, {"BIG_ORDER_THRESHOLD": "100", "ORDERS_TABLE": "Orders"}, clear=True)
    @patch("lambda_function.dynamodb")
    def test_dynamodb_error_returns_500(self, mock_dynamodb):
        mock_table = MagicMock()
        error = ClientError(
            error_response={"Error": {"Code": "500", "Message": "Boom"}},
            operation_name="PutItem",
        )
        mock_table.put_item.side_effect = error
        mock_dynamodb.Table.return_value = mock_table

        response = lambda_function.lambda_handler(self.base_event, None)

        self.assertEqual(response["statusCode"], 500)


if __name__ == "__main__":
    unittest.main()