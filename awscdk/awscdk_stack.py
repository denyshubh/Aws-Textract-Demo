from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_events as events,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as snsSubscriptions,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    aws_s3,
)
from aws_cdk.aws_lambda_event_sources import (
    S3EventSource,
    SqsEventSource,
    SnsEventSource,
    DynamoEventSource,
)
from aws_cdk.aws_events_targets import LambdaFunction


class AwscdkStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here
