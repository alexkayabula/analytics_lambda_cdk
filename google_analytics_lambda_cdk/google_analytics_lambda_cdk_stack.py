from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    Stack
)

class GoogleAnalyticsLambdaCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #  Defines an AWS Lambda resource
        google_analytics_to_s3_lambda = _lambda.Function(
            self, 'GoogleAnalyticsToS3Handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='google_analytics_to_s3.handler',
            timeout=Duration.seconds(180)
        )

        s3_to_postgresql_lambda = _lambda.Function(
            self, 'S3ToPostgresHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('./lambda'),
            handler='s3_to_postgresql.handler',
            timeout=Duration.seconds(180)
        )

        # Run every day at 6PM UTC
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html

        rule_one = events.Rule(
            self,  "RuleOne",
            schedule=events.Schedule.cron(
                minute='5',
                hour='8',
                month='*',
                week_day='MON-SUN',
                year="*",
            )
        )

        rule_two = events.Rule(
            self, "RuleTwo",
            schedule=events.Schedule.cron(
                minute='10',
                hour='8',
                month='*',
                week_day='MON-SUN',
                year='*'
            )
        )

        rule_one.add_target(targets.LambdaFunction(google_analytics_to_s3_lambda))
        rule_two.add_target(targets.LambdaFunction(s3_to_postgresql_lambda))
