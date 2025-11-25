#!/usr/bin/env python3
import aws_cdk as cdk

from cdk_csv_app.cdk_csv_app_stack import CdkCsvAppStack

app = cdk.App()

env = cdk.Environment(
    account="521271295481",  # npr. "123456789012"
    region="eu-central-1",
)

CdkCsvAppStack(app, "CdkCsvAppStack", env=env)

app.synth()
