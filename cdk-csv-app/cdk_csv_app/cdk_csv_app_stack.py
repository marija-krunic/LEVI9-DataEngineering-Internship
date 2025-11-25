from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
)

class CdkCsvAppStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1) Napravimo S3 bucket-e
        input_bucket = s3.Bucket(self, "InputBucket")
        output_bucket = s3.Bucket(self, "OutputBucket")

        # 2) Lambda iz Docker image-a
        #    CDK će sam uraditi docker build i push u ECR
        csv_lambda = _lambda.DockerImageFunction(
            self,
            "CsvProcessorLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                # Putanja do foldera gde je Dockerfile, GLEDANO iz cdk-csv-app foldera
                "../custom-lambda-image/lambda-image"
            ),
            timeout=Duration.seconds(30),
            memory_size=512,
            environment={
                "INPUT_BUCKET": input_bucket.bucket_name,
                "OUTPUT_BUCKET": output_bucket.bucket_name,
                "OUTPUT_PREFIX": "processed/",
            },
        )

        # 3) Dozvole da Lambda čita/piše S3
        input_bucket.grant_read(csv_lambda)
        output_bucket.grant_write(csv_lambda)
