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

      # Create an S3 bucket
        s3_bucket = s3.Bucket(
            self, "MyS3Bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_name="my-s3-bucket-2024",
            )
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

        #  Create an IAM role for the Lambda functions
        lambda_role = iam.Role(
            self, "MyLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess")
            ]
        )
        #  Add permissions to the IAM role for the Lambda functions to access s3 bucket
        s3_bucket.grant_read_write(lambda_role)

        # Create an IAM role for the RDS Instance
        rds_import_role = iam.Role(
            self, "RDSImportRole",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com")
        )

        # Add policy to access s3 bucket
        rds_import_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[s3_bucket.bucket_arn, s3_bucket.bucket_arn + "/*"]
        ))

         #  Add permissions to the IAM role for the RDS Instance to import s3 data.
        s3_bucket.grant_read_write(rds_import_role)

        # Secret with username and password fields 
        # Exclude characters to prevent errors when generating automatic password.
        secret = secretsmanager.Secret(
            self, "Secret",
            secret_name="mysecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "postgres"}),
                generate_string_key="password",
                exclude_characters=" %+~`#$&*()|[]{}:;<>?!'/@\"\\"
            )
        )

       # Add permission for the IAM role to access AWS Secret Manager secret.
        secret.grant_read(lambda_role)
        secret.grant_write(lambda_role)


        # Create a VPC
        vpc = ec2.Vpc(self, "Vpc", nat_gateways=0)

        # Cost optimization
        # Reference: https://aws.amazon.com/premiumsupport/knowledge-center/vpc-reduce-nat-gateway-transfer-costs/
        # Reference: https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html
        # Reference: https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html

        # Create a gateway VPC endpoint for S3
        s3_endpoint = vpc.add_gateway_endpoint("S3Endpoint",
        service=ec2.GatewayVpcEndpointAwsService.S3
        )

       # Add and customize policy to access s3
        s3_endpoint.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[s3_bucket.bucket_arn, s3_bucket.bucket_arn + "/*"],
            principals=[iam.AnyPrincipal()]
        ))

        # Create an RDS instance
        security_group = ec2.SecurityGroup(self, "SecurityGroup", vpc=vpc)
        rds_instance = rds.DatabaseInstance(self, "RDSInstance",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_13),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
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
            s3_import_role=rds_import_role
        )
        rds_instance.connections.allow_default_port_from_any_ipv4()

        #  Defines an AWS Lambda resources
        google_analytics_to_s3_lambda = _lambda.Function(
            self, 'GoogleAnalyticsToS3Handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='google_analytics_to_s3.handler',
            timeout=Duration.seconds(180),
            role=lambda_role
        )

        s3_to_postgresql_lambda = _lambda.Function(
            self, 'S3ToPostgresHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='s3_to_postgresql.handler',
            timeout=Duration.seconds(180),
            role=lambda_role
        )

        # Schedule lambdas to run every day at specific time.
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html

        rule_one = events.Rule(
            self,  "RuleOne",
            schedule=events.Schedule.cron(
                minute='20',
                hour='21',
                month='*',
                week_day='MON-SUN',
                year="*",
            )
        )

        rule_two = events.Rule(
            self, "RuleTwo",
            schedule=events.Schedule.cron(
                minute='20',
                hour='21',
                month='*',
                week_day='MON-SUN',
                year='*'
            )
        )

        rule_one.add_target(targets.LambdaFunction(google_analytics_to_s3_lambda))
        rule_two.add_target(targets.LambdaFunction(s3_to_postgresql_lambda))
