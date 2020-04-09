from aws_cdk import core
from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as events,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as snsSubscriptions,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
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

        # ********* SNS Topics *************
        jobCompletionTopic = sns.Topic(self, "JobCompletion")

        # **********IAM Roles******************************
        textractServiceRole = iam.Role(
            self,
            "TextractServiceRole",
            assumed_by=iam.ServicePrincipal("textract.amazonaws.com"),
        )
        textractServiceRole.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[jobCompletionTopic.topic_arn],
                actions=["sns:Publish"],
            )
        )
        comprehendServiceRole = iam.Role(
            self,
            "ComprehendServiceRole",
            assumed_by=iam.ServicePrincipal("comprehend.amazonaws.com"),
        )
        comprehendServiceRole.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "comprehend:*",
                    "s3:ListAllMyBuckets",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "iam:ListRoles",
                    "iam:GetRole",
                ],
            )
        )
        # **********S3 Batch Operations Role******************************
        s3BatchOperationsRole = iam.Role(
            self,
            "S3BatchOperationsRole",
            assumed_by=iam.ServicePrincipal("batchoperations.s3.amazonaws.com"),
        )

        # **********S3 Bucket******************************
        # S3 bucket for input documents and output
        contentBucket = s3.Bucket(self, "DocumentsBucket", versioned=False)

        existingContentBucket = s3.Bucket(
            self, "ExistingDocumentsBucket", versioned=False
        )
        existingContentBucket.grant_read_write(s3BatchOperationsRole)

        inventoryAndLogsBucket = s3.Bucket(
            self, "InventoryAndLogsBucket", versioned=False
        )
        inventoryAndLogsBucket.grant_read_write(s3BatchOperationsRole)

        # **********DynamoDB Table*************************
        # DynamoDB table with links to output in S3
        outputTable = dynamodb.Table(
            self,
            "OutputTable",
            partition_key={
                "name": "documentId",
                "type": dynamodb.AttributeType.STRING,
            },
            sort_key={"name": "outputType", "type": dynamodb.AttributeType.STRING,},
        )

        # DynamoDB table with links to output in S3
        documentsTable = dynamodb.Table(
            self,
            "DocumentsTable",
            partition_key={
                "name": "documentId",
                "type": dynamodb.AttributeType.STRING,
            },
            stream=dynamodb.StreamViewType.NEW_IMAGE,
        )

        # **********SQS Queues*****************************
        # DLQ (Dead Letter Queue)
        dlq = sqs.Queue(
            self,
            "DLQ",
            visibility_timeout=core.Duration.seconds(30),
            retention_period=core.Duration.seconds(1209600),
        )

        # Input Queue for sync jobs
        syncJobsQueue = sqs.Queue(
            self,
            "SyncJobs",
            visibility_timeout=core.Duration.seconds(30),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue={"queue": dlq, "max_receive_count": 50},
        )
        # Input Queue for async jobs
        asyncJobsQueue = sqs.Queue(
            self,
            "AsyncJobs",
            visibility_timeout=core.Duration.seconds(30),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue={"queue": dlq, "max_receive_count": 50},
        )

        # Queue
        jobResultsQueue = sqs.Queue(
            self,
            "JobResults",
            visibility_timeout=core.Duration.seconds(900),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue={"queue": dlq, "max_receive_count": 50},
        )
        # Trigger
        # jobCompletionTopic.subscribeQueue(jobResultsQueue)
        jobCompletionTopic.add_subscription(
            snsSubscriptions.SqsSubscription(jobResultsQueue)
        )

        # **********Lambda Functions******************************

        # Helper Layer with helper functions
        helperLayer = _lambda.LayerVersion(
            self,
            "HelperLayer",
            code=_lambda.Code.from_asset("awscdk/lambda/helper"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
            license="Apache-2.0",
            description="Helper layer.",
        )

        # Textractor helper layer
        textractorLayer = _lambda.LayerVersion(
            self,
            "Textractor",
            code=_lambda.Code.from_asset("awscdk/lambda/textractor"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
            license="Apache-2.0",
            description="Textractor layer.",
        )

        # -----------------------------------------------------------

        # S3 Event processor
        s3Processor = _lambda.Function(
            self,
            "S3Processor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/s3processor"),
            handler="lambda_function.lambda_handler",
            environment={
                "SYNC_QUEUE_URL": syncJobsQueue.queue_url,
                "ASYNC_QUEUE_URL": asyncJobsQueue.queue_url,
                "DOCUMENTS_TABLE": documentsTable.table_name,
                "OUTPUT_TABLE": outputTable.table_name,
            },
        )
        # Layer
        s3Processor.add_layers(helperLayer)
        # Trigger
        s3Processor.add_event_source(
            S3EventSource(contentBucket, events=[s3.EventType.OBJECT_CREATED])
        )
        # Permissions
        documentsTable.grant_read_write_data(s3Processor)
        syncJobsQueue.grant_send_messages(s3Processor)
        asyncJobsQueue.grant_send_messages(s3Processor)

        # ------------------------------------------------------------

        # S3 Batch Operations Event processor
        s3BatchProcessor = _lambda.Function(
            self,
            "S3BatchProcessor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/s3batchprocessor"),
            handler="lambda_function.lambda_handler",
            environment={
                "DOCUMENTS_TABLE": documentsTable.table_name,
                "OUTPUT_TABLE": outputTable.table_name,
            },
            reserved_concurrent_executions=1,
        )
        # Layer
        s3BatchProcessor.add_layers(helperLayer)
        # Permissions
        documentsTable.grant_read_write_data(s3BatchProcessor)
        s3BatchProcessor.grant_invoke(s3BatchOperationsRole)
        s3BatchOperationsRole.add_to_policy(
            iam.PolicyStatement(actions=["lambda:*"], resources=["*"])
        )

        # ------------------------------------------------------------

        # Document processor (Router to Sync/Async Pipeline)
        documentProcessor = _lambda.Function(
            self,
            "TaskProcessor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/documentprocessor"),
            handler="lambda_function.lambda_handler",
            environment={
                "SYNC_QUEUE_URL": syncJobsQueue.queue_url,
                "ASYNC_QUEUE_URL": asyncJobsQueue.queue_url,
            },
        )
        # Layer
        documentProcessor.add_layers(helperLayer)
        # Trigger
        documentProcessor.add_event_source(
            DynamoEventSource(
                documentsTable, starting_position=_lambda.StartingPosition.TRIM_HORIZON,
            )
        )

        # Permissions
        documentsTable.grant_read_write_data(documentProcessor)
        syncJobsQueue.grant_send_messages(documentProcessor)
        asyncJobsQueue.grant_send_messages(documentProcessor)

        # ------------------------------------------------------------

        # Sync Jobs Processor (Process jobs using sync APIs)
        syncProcessor = _lambda.Function(
            self,
            "SyncProcessor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/documentprocessor"),
            handler="lambda_function.lambda_handler",
            environment={
                "OUTPUT_TABLE": outputTable.table_name,
                "DOCUMENTS_TABLE": documentsTable.table_name,
                "AWS_DATA_PATH": "models",
            },
            reserved_concurrent_executions=1,
            timeout=core.Duration.seconds(25),
        )
        # Layer
        syncProcessor.add_layers(helperLayer)
        syncProcessor.add_layers(textractorLayer)
        # Trigger
        syncProcessor.add_event_source(SqsEventSource(syncJobsQueue, batch_size=1))
        # Permissions
        contentBucket.grant_read_write(syncProcessor)
        existingContentBucket.grant_read_write(syncProcessor)
        outputTable.grant_read_write_data(syncProcessor)
        documentsTable.grant_read_write_data(syncProcessor)
        syncProcessor.add_to_role_policy(
            iam.PolicyStatement(actions=["textract:*"], resources=["*"])
        )

        # ------------------------------------------------------------

        # Async Job Processor (Start jobs using Async APIs)
        asyncProcessor = _lambda.Function(
            self,
            "ASyncProcessor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/asyncprocessor"),
            handler="lambda_function.lambda_handler",
            environment={
                "ASYNC_QUEUE_URL": asyncJobsQueue.queue_url,
                "SNS_TOPIC_ARN": jobCompletionTopic.topic_arn,
                "SNS_ROLE_ARN": textractServiceRole.role_arn,
                "AWS_DATA_PATH": "models",
            },
            reserved_concurrent_executions=1,
            timeout=core.Duration.seconds(60),
        )
        # asyncProcessor.addEnvironment("SNS_TOPIC_ARN", textractServiceRole.topic_arn)
        # Layer
        asyncProcessor.add_layers(helperLayer)
        # Triggers
        # Run async job processor every 5 minutes
        # Enable code below after test deploy
        rule = events.Rule(
            self, "Rule", schedule=events.Schedule.expression("rate(2 minutes)")
        )
        rule.add_target(LambdaFunction(asyncProcessor))

        # Run when a job is successfully complete
        asyncProcessor.add_event_source(SnsEventSource(jobCompletionTopic))
        # Permissions
        contentBucket.grant_read(asyncProcessor)
        existingContentBucket.grant_read_write(asyncProcessor)
        asyncJobsQueue.grant_consume_messages(asyncProcessor)
        asyncProcessor.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"], resources=[textractServiceRole.role_arn],
            )
        )
        asyncProcessor.add_to_role_policy(
            iam.PolicyStatement(actions=["textract:*"], resources=["*"])
        )
        # ------------------------------------------------------------

        # Async Jobs Results Processor
        jobResultProcessor = _lambda.Function(
            self,
            "JobResultProcessor",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("awscdk/lambda/jobresultprocessor"),
            handler="lambda_function.lambda_handler",
            memory_size=2000,
            reserved_concurrent_executions=50,
            timeout=core.Duration.seconds(900),
            environment={
                "OUTPUT_TABLE": outputTable.table_name,
                "DOCUMENTS_TABLE": documentsTable.table_name,
                "AWS_DATA_PATH": "models",
            },
        )
        # Layer
        jobResultProcessor.add_layers(helperLayer)
        jobResultProcessor.add_layers(textractorLayer)
        # Triggers
        jobResultProcessor.add_event_source(
            SqsEventSource(jobResultsQueue, batch_size=1)
        )
        # Permissions
        outputTable.grant_read_write_data(jobResultProcessor)
        documentsTable.grant_read_write_data(jobResultProcessor)
        contentBucket.grant_read_write(jobResultProcessor)
        existingContentBucket.grant_read_write(jobResultProcessor)
        jobResultProcessor.add_to_role_policy(
            iam.PolicyStatement(actions=["textract:*", "comprehend:*"], resources=["*"])
        )

        # --------------
        # PDF Generator
        pdfGenerator = _lambda.Function(
            self,
            "PdfGenerator",
            runtime=_lambda.Runtime.JAVA_8,
            code=_lambda.Code.from_asset("awscdk/lambda/pdfgenerator"),
            handler="DemoLambdaV2::handleRequest",
            memory_size=3000,
            timeout=core.Duration.seconds(900),
        )
        contentBucket.grant_read_write(pdfGenerator)
        existingContentBucket.grant_read_write(pdfGenerator)
        pdfGenerator.grant_invoke(syncProcessor)
        pdfGenerator.grant_invoke(asyncProcessor)
