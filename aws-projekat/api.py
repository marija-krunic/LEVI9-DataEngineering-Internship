import os
import json
from datetime import date
import requests
import boto3
from botocore.exceptions import ClientError

# Konstante iz zadatka
API_URL = "https://rq5fbome43vbdgq7xoe7d6wbwa0ngkgr.lambda-url.eu-west-1.on.aws/"
SECRET_NAME = "tourist_estimate_token"
AWS_REGION = "eu-west-1"  # ako vam je stvarni region npr. "eu-west-1", ovde stavi to


def get_token_from_secrets_manager() -> str:
    """
    Čita token iz AWS Secrets Manager-a.
    Secret se zove 'tourist_estimate_token'.
    """
    client = boto3.client("secretsmanager", region_name="eu-west-1")

    try:
        response = client.get_secret_value(SecretId=SECRET_NAME)
    except ClientError as e:
        # Ovde možeš da loguješ ili baciš dalje
        raise RuntimeError(f"Error reading secret {SECRET_NAME}: {e}") from e

    # Secret može biti u SecretString ili u binarnom obliku
    if "SecretString" in response:
        secret_value = response["SecretString"]
    else:
        secret_value = response["SecretBinary"].decode("utf-8")

    # Ako je secret JSON, ovde bi ga parsirala, ali u tvom slučaju je verovatno plain string
    return secret_value


def fetch_tourist_estimates(for_date: str):
    """
    Šalje GET zahtev na API za dati datum (YYYY-MM-DD)
    i vraća parsiran JSON odgovor.
    """
    token = get_token_from_secrets_manager()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "date": for_date
    }

    response = requests.get(API_URL, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"API returned status {response.status_code}: {response.text}"
        )

    data = response.json()
    return data


def main():
    # Možeš da probaš za današnji datum,
    # ili staviš neki konkretan, npr. "2025-01-01"
    today_str = date.today().strftime("%Y-%m-%d")

    data = fetch_tourist_estimates(today_str)

    # Lepo ispiši rezultat
    print(f"Tourist estimates for date: {data.get('for_date')}")
    for city in data.get("info", []):
        name = city.get("name")
        visitors = city.get("estimated_no_people")
        print(f"- {name}: {visitors} people")


if __name__ == "__main__":
    main()
