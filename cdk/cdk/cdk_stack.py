from aws_cdk import (
    aws_lambda,
    BundlingOptions,
    aws_events,
    aws_events_targets,
    # aws_rds,
    aws_sns,
    aws_sns_subscriptions,
    Stack
)
from constructs import Construct

from cdk.env_vars import ibmsm_env_vars


class IbmsmStack(Stack):
    """
    Transfers 87 files;
        Lambda 1: 'IbmsmCronChecker':
            - checks if there are new files that have not yet been processed
            - if no; quits; otherwise adds to process db and publishes sns event
        Lambda 2: 'IbmsmProcessLoop':
            - listens for sns event; processes first unprocessed file and calls itself
            - no files? publishes event notification
    """

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        sns_topic = aws_sns.Topic(self, "IbmsmProcessLoopTopic")

        env_vars = ibmsm_env_vars(self, 'Ibmsm')

        sqlalchemy_layer = aws_lambda.LayerVersion(self, "SqlAlchemyLayer",
            code=aws_lambda.Code.from_asset("sqlalchemy"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            description="Shared SQLAlchemy Layer"
        )
        
        sftp_layer = aws_lambda.LayerVersion(self, "SftpLayer",
            code=aws_lambda.Code.from_asset("sftp"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            description="Shared Sftp Layer"
        )

        ibmsm_cron_checker = aws_lambda.Function(
            self, "IbmsmCronChecker",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=aws_lambda.Code.from_asset(
                "ibmsm_cron_checker",
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r ibmsm_cron_checker_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            memory_size=128,
            layers=[sqlalchemy_layer, sftp_layer],
            
            environment={
                "SNS_TOPIC_ARN": sns_topic.topic_arn,
                **env_vars
            }
        )
        sns_topic.grant_publish(ibmsm_cron_checker)
        cron_rule = aws_events.Rule(
            self, "CronRule",
            schedule=aws_events.Schedule.cron(minute="0", hour="12"),
        )
        cron_rule.add_target(aws_events_targets.LambdaFunction(ibmsm_cron_checker))


        ibmsm_process_loop = aws_lambda.Function(
            self, "IbmsmProcessLoop",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=aws_lambda.Code.from_asset(
                "ibmsm_process_loop",
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r ibmsm_process_loop_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            memory_size=2048,
            layers=[sqlalchemy_layer, sftp_layer],
            environment=env_vars
        )
        sns_topic.add_subscription(aws_sns_subscriptions.LambdaSubscription(ibmsm_process_loop))

