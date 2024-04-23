#!/usr/bin/env python3
import os

import aws_cdk

from cdk.cdk_stack import IbmsmStack


app = aws_cdk.App()
IbmsmStack(app, "IbmsmStack",
    env=aws_cdk.Environment(region='ca-central-1')
)

app.synth()
