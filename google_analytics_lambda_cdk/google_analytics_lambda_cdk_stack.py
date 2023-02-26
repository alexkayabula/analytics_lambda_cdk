import json
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_secretsmanager as secretsmanager,
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_rds as rds,
    aws_s3 as s3,
    aws_ec2 as ec2,
    Stack,
    RemovalPolicy,
)

class GoogleAnalyticsLambdaCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an IAM role for importing data from S3
        rds_role = iam.Role(self, "ImportRole",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com")
        )
        rds_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[bucket.bucket_arn, bucket.bucket_arn + "/*"]
        ))

        # Simple secret
        secret = secretsmanager.Secret(self, "Secret")

        # Create an RDS instance
        vpc = ec2.Vpc(self, "Vpc")
        security_group = ec2.SecurityGroup(self, "SecurityGroup", vpc=vpc)
        rds_instance = rds.DatabaseInstance(self, "RDSInstance",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_13),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE2, ec2.InstanceSize.MICRO),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[security_group],
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            allocated_storage=10,
            database_name="mydatabase",
            instance_identifier="mydbinstance",
            port=5432,
            credentials=rds.Credentials.from_secret(secret),
            s3_import_role=rds_role
        )

        # Create an S3 bucket
        s3_bucket = s3.Bucket(self, "MyS3Bucket")
        s3_bucket.add_to_resource_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject"],
            resources=[s3_bucket.arn_for_objects("*")],
            principals=[iam.AnyPrincipal()]
        ))
        s3_bucket.add_to_resource_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket"],
            resources=[s3_bucket.bucket_arn],
            principals=[iam.AnyPrincipal()]
        ))

        # Create an IAM role for the RDS instance to access S3
        rds_role = iam.Role(
            self, "RDSRole",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
        )

        # Grant the IAM role permissions to access the S3 bucket
        s3_bucket.grant_read_write(rds_role)


        #  Create an IAM role for the Lambda functions
        role = iam.Role(
            self, "MyLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess")
            ]
        )

        #  Add permissions to the IAM role for the Lambda functions
        s3_bucket.grant_read_write(role)


        #  Defines an AWS Lambda resource
        google_analytics_to_s3_lambda = _lambda.Function(
            self, 'GoogleAnalyticsToS3Handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='google_analytics_to_s3.handler',
            timeout=Duration.seconds(180),
            role=role
        )

        s3_to_postgresql_lambda = _lambda.Function(
            self, 'S3ToPostgresHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='s3_to_postgresql.handler',
            timeout=Duration.seconds(180),
            role=role
        )

        # Run every day at 6PM UTC
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html

        rule_one = events.Rule(
            self,  "RuleOne",
            schedule=events.Schedule.cron(
                minute='20',
                hour='10',
                month='*',
                week_day='MON-SUN',
                year="*",
            )
        )

        rule_two = events.Rule(
            self, "RuleTwo",
            schedule=events.Schedule.cron(
                minute='25',
                hour='10',
                month='*',
                week_day='MON-SUN',
                year='*'
            )
        )

        rule_one.add_target(targets.LambdaFunction(google_analytics_to_s3_lambda))
        rule_two.add_target(targets.LambdaFunction(s3_to_postgresql_lambda))
