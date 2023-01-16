#!/usr/bin/env python3

import aws_cdk as cdk

from google_analytics_lambda_cdk.google_analytics_lambda_cdk_stack import GoogleAnalyticsLambdaCdkStack


app = cdk.App()
GoogleAnalyticsLambdaCdkStack(app, "google-analytics-lambda-cdk")

app.synth()
