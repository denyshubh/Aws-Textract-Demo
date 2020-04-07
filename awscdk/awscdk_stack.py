from aws_cdk import (
    core as cdk,
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
            {"assumedBy": iam.ServicePrincipal("textract.amazonaws.com")},
        )
        textractServiceRole.addToPolicy(
            iam.PolicyStatement(
                {
                    "effect": iam.Effect.ALLOW,
                    "resources": [jobCompletionTopic.topicArn],
                    "actions": ["sns:Publish"],
                }
            )
        )
        # **********S3 Batch Operations Role******************************
        s3BatchOperationsRole = iam.Role(
            self,
            "S3BatchOperationsRole",
            {"assumedBy": iam.ServicePrincipal("batchoperations.s3.amazonaws.com")},
        )

        # **********S3 Bucket******************************
        # S3 bucket for input documents and output
        contentBucket = s3.Bucket(self, "DocumentsBucket", {"versioned": False})

        existingContentBucket = s3.Bucket(
            self, "ExistingDocumentsBucket", {"versioned": False}
        )
        existingContentBucket.grantReadWrite(s3BatchOperationsRole)

        inventoryAndLogsBucket = s3.Bucket(
            self, "InventoryAndLogsBucket", {"versioned": False}
        )
        inventoryAndLogsBucket.grantReadWrite(s3BatchOperationsRole)

        # **********DynamoDB Table*************************
        # DynamoDB table with links to output in S3
        outputTable = dynamodb.Table(
            self,
            "OutputTable",
            {
                "partitionKey": {
                    "name": "documentId",
                    "type": dynamodb.AttributeType.STRING,
                },
                "sortKey": {
                    "name": "outputType",
                    "type": dynamodb.AttributeType.STRING,
                },
            },
        )

        # DynamoDB table with links to output in S3
        documentsTable = dynamodb.Table(
            self,
            "DocumentsTable",
            {
                "partitionKey": {
                    "name": "documentId",
                    "type": dynamodb.AttributeType.STRING,
                },
                "stream": dynamodb.StreamViewType.NEW_IMAGE,
            },
        )

        # **********SQS Queues*****************************
        # DLQ (Dead Letter Queue)
        dlq = sqs.Queue(
            self,
            "DLQ",
            {
                "visibilityTimeout": cdk.Duration.seconds(30),
                "retentionPeriod": cdk.Duration.seconds(1209600),
            },
        )

        # Input Queue for sync jobs
        syncJobsQueue = sqs.Queue(
            self,
            "SyncJobs",
            {
                "visibilityTimeout": cdk.Duration.seconds(30),
                "retentionPeriod": cdk.Duration.seconds(1209600),
                "deadLetterQueue": {"queue": dlq, "maxReceiveCount": 50},
            },
        )
        # Input Queue for async jobs
        asyncJobsQueue = sqs.Queue(
            self,
            "AsyncJobs",
            {
                "visibilityTimeout": cdk.Duration.seconds(30),
                "retentionPeriod": cdk.Duration.seconds(1209600),
                "deadLetterQueue": {"queue": dlq, "maxReceiveCount": 50},
            },
        )

        # Queue
        jobResultsQueue = sqs.Queue(
            self,
            "JobResults",
            {
                "visibilityTimeout": cdk.Duration.seconds(900),
                "retentionPeriod": cdk.Duration.seconds(1209600),
                "deadLetterQueue": {"queue": dlq, "maxReceiveCount": 50},
            },
        )
        # Trigger
        # jobCompletionTopic.subscribeQueue(jobResultsQueue)
        jobCompletionTopic.addSubscription(
            snsSubscriptions.SqsSubscription(jobResultsQueue)
        )

        # **********Lambda Functions******************************

        # Helper Layer with helper functions
        helperLayer = _lambda.LayerVersion(
            self,
            "HelperLayer",
            {
                "code": _lambda.Code.fromAsset("lambda/helper"),
                "compatibleRuntimes": [_lambda.Runtime.PYTHON_3_7],
                "license": "Apache-2.0",
                "description": "Helper layer.",
            },
        )

        # Textractor helper layer
        textractorLayer = _lambda.LayerVersion(
            self,
            "Textractor",
            {
                "code": _lambda.Code.fromAsset("lambda/textractor"),
                "compatibleRuntimes": [_lambda.Runtime.PYTHON_3_7],
                "license": "Apache-2.0",
                "description": "Textractor layer.",
            },
        )

        # -----------------------------------------------------------

        # S3 Event processor
        s3Processor = _lambda.Function(
            self,
            "S3Processor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/s3processor"),
                "handler": "lambda_function.lambda_handler",
                "environment": {
                    "SYNC_QUEUE_URL": syncJobsQueue.queueUrl,
                    "ASYNC_QUEUE_URL": asyncJobsQueue.queueUrl,
                    "DOCUMENTS_TABLE": documentsTable.tableName,
                    "OUTPUT_TABLE": outputTable.tableName,
                },
            },
        )
        # Layer
        s3Processor.addLayers(helperLayer)
        # Trigger
        s3Processor.addEventSource(
            S3EventSource(contentBucket, {"events": [s3.EventType.OBJECT_CREATED]})
        )
        # Permissions
        documentsTable.grantReadWriteData(s3Processor)
        syncJobsQueue.grantSendMessages(s3Processor)
        asyncJobsQueue.grantSendMessages(s3Processor)

        # ------------------------------------------------------------

        # S3 Batch Operations Event processor
        s3BatchProcessor = _lambda.Function(
            self,
            "S3BatchProcessor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/s3batchprocessor"),
                "handler": "lambda_function.lambda_handler",
                "environment": {
                    "DOCUMENTS_TABLE": documentsTable.tableName,
                    "OUTPUT_TABLE": outputTable.tableName,
                },
                "reservedConcurrentExecutions": 1,
            },
        )
        # Layer
        s3BatchProcessor.addLayers(helperLayer)
        # Permissions
        documentsTable.grantReadWriteData(s3BatchProcessor)
        s3BatchProcessor.grantInvoke(s3BatchOperationsRole)
        s3BatchOperationsRole.addToPolicy(
            iam.PolicyStatement({"actions": ["lambda:*"], "resources": ["*"]})
        )

        # ------------------------------------------------------------

        # Document processor (Router to Sync/Async Pipeline)
        documentProcessor = _lambda.Function(
            self,
            "TaskProcessor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/documentprocessor"),
                "handler": "lambda_function.lambda_handler",
                "environment": {
                    "SYNC_QUEUE_URL": syncJobsQueue.queueUrl,
                    "ASYNC_QUEUE_URL": asyncJobsQueue.queueUrl,
                },
            },
        )
        # Layer
        documentProcessor.addLayers(helperLayer)
        # Trigger
        documentProcessor.addEventSource(
            DynamoEventSource(
                documentsTable,
                {"startingPosition": _lambda.StartingPosition.TRIM_HORIZON},
            )
        )

        # Permissions
        documentsTable.grantReadWriteData(documentProcessor)
        syncJobsQueue.grantSendMessages(documentProcessor)
        asyncJobsQueue.grantSendMessages(documentProcessor)

        # ------------------------------------------------------------

        # Sync Jobs Processor (Process jobs using sync APIs)
        syncProcessor = _lambda.Function(
            self,
            "SyncProcessor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/syncprocessor"),
                "handler": "lambda_function.lambda_handler",
                "reservedConcurrentExecutions": 1,
                "timeout": cdk.Duration.seconds(25),
                "environment": {
                    "OUTPUT_TABLE": outputTable.tableName,
                    "DOCUMENTS_TABLE": documentsTable.tableName,
                    "AWS_DATA_PATH": "models",
                },
            },
        )
        # Layer
        syncProcessor.addLayers(helperLayer)
        syncProcessor.addLayers(textractorLayer)
        # Trigger
        syncProcessor.addEventSource(SqsEventSource(syncJobsQueue, {"batchSize": 1}))
        # Permissions
        contentBucket.grantReadWrite(syncProcessor)
        existingContentBucket.grantReadWrite(syncProcessor)
        outputTable.grantReadWriteData(syncProcessor)
        documentsTable.grantReadWriteData(syncProcessor)
        syncProcessor.addToRolePolicy(
            iam.PolicyStatement({"actions": ["textract:*"], "resources": ["*"]})
        )

        # ------------------------------------------------------------

        # Async Job Processor (Start jobs using Async APIs)
        asyncProcessor = _lambda.Function(
            self,
            "ASyncProcessor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/asyncprocessor"),
                "handler": "lambda_function.lambda_handler",
                "reservedConcurrentExecutions": 1,
                "timeout": cdk.Duration.seconds(60),
                "environment": {
                    "ASYNC_QUEUE_URL": asyncJobsQueue.queueUrl,
                    "SNS_TOPIC_ARN": jobCompletionTopic.topicArn,
                    "SNS_ROLE_ARN": textractServiceRole.roleArn,
                    "AWS_DATA_PATH": "models",
                },
            },
        )
        # asyncProcessor.addEnvironment("SNS_TOPIC_ARN", textractServiceRole.topicArn)
        # Layer
        asyncProcessor.addLayers(helperLayer)
        # Triggers
        # Run async job processor every 5 minutes
        # Enable code below after test deploy
        rule = events.Rule(
            self, "Rule", {"schedule": events.Schedule.expression("rate(2 minutes)")}
        )
        rule.addTarget(LambdaFunction(asyncProcessor))

        # Run when a job is successfully complete
        asyncProcessor.addEventSource(SnsEventSource(jobCompletionTopic))
        # Permissions
        contentBucket.grantRead(asyncProcessor)
        existingContentBucket.grantReadWrite(asyncProcessor)
        asyncJobsQueue.grantConsumeMessages(asyncProcessor)
        asyncProcessor.addToRolePolicy(
            iam.PolicyStatement(
                {
                    "actions": ["iam:PassRole"],
                    "resources": [textractServiceRole.roleArn],
                }
            )
        )
        asyncProcessor.addToRolePolicy(
            iam.PolicyStatement({"actions": ["textract:*"], "resources": ["*"]})
        )
        # ------------------------------------------------------------

        # Async Jobs Results Processor
        jobResultProcessor = _lambda.Function(
            self,
            "JobResultProcessor",
            {
                "runtime": _lambda.Runtime.PYTHON_3_7,
                "code": _lambda.Code.asset("lambda/jobresultprocessor"),
                "handler": "lambda_function.lambda_handler",
                "memorySize": 2000,
                "reservedConcurrentExecutions": 50,
                "timeout": cdk.Duration.seconds(900),
                "environment": {
                    "OUTPUT_TABLE": outputTable.tableName,
                    "DOCUMENTS_TABLE": documentsTable.tableName,
                    "AWS_DATA_PATH": "models",
                },
            },
        )
        # Layer
        jobResultProcessor.addLayers(helperLayer)
        jobResultProcessor.addLayers(textractorLayer)
        # Triggers
        jobResultProcessor.addEventSource(
            SqsEventSource(jobResultsQueue, {"batchSize": 1})
        )
        # Permissions
        outputTable.grantReadWriteData(jobResultProcessor)
        documentsTable.grantReadWriteData(jobResultProcessor)
        contentBucket.grantReadWrite(jobResultProcessor)
        existingContentBucket.grantReadWrite(jobResultProcessor)
        jobResultProcessor.addToRolePolicy(
            iam.PolicyStatement({"actions": ["textract:*"], "resources": ["*"]})
        )

        # --------------
        # PDF Generator
        pdfGenerator = _lambda.Function(
            self,
            "PdfGenerator",
            {
                "runtime": _lambda.Runtime.JAVA_8,
                "code": _lambda.Code.asset("lambda/pdfgenerator"),
                "handler": "DemoLambdaV2::handleRequest",
                "memorySize": 3000,
                "timeout": cdk.Duration.seconds(900),
            },
        )
        contentBucket.grantReadWrite(pdfGenerator)
        existingContentBucket.grantReadWrite(pdfGenerator)
        pdfGenerator.grantInvoke(syncProcessor)
        pdfGenerator.grantInvoke(asyncProcessor)
