import os
import urllib.parse
from dataclasses import dataclass

from blazel.serialize import Serializable


@dataclass
class LambdaContext(Serializable):
    execution_env: str | None = None
    default_region: str | None = None
    function_name: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    memory_limit_in_mb: str | None = None
    log_group_name: str | None = None
    log_stream_name: str | None = None
    aws_request_id: str | None = None

    @classmethod
    def from_context(cls, context):
        lambda_context = cls(
            execution_env=os.environ.get('AWS_EXECUTION_ENV'),
            default_region=os.environ.get('AWS_DEFAULT_REGION'),
            function_name=context.function_name,
            function_version=context.function_version,
            invoked_function_arn=context.invoked_function_arn,
            memory_limit_in_mb=context.memory_limit_in_mb,
            log_group_name=context.log_group_name,
            log_stream_name=context.log_stream_name,
            aws_request_id=context.aws_request_id
        )
        return lambda_context

    @property
    def cloudwatch_link(self) -> str | None:
        if self.default_region is None or self.log_group_name is None or self.log_stream_name is None or self.aws_request_id is None:
            return None
        return self.get_cloudwatch_link(
            self.default_region,
            self.log_group_name,
            self.log_stream_name,
            self.aws_request_id
        )

    @staticmethod
    def get_cloudwatch_link(
            region: str,
            log_group_name: str,
            log_stream_name: str,
            aws_request_id: str
    ) -> str:
        """
        Generate a deep link to the specific Lambda function log in AWS CloudWatch Logs.
        """
        encoded_log_group = urllib.parse.quote(log_group_name, safe='')
        encoded_log_stream = urllib.parse.quote(log_stream_name, safe='')
        filter_pattern = urllib.parse.quote(f'"{aws_request_id}"', safe='')
        return (
            f'https://{region}.console.aws.amazon.com/cloudwatch/home'
            f'?region={region}'
            f'#logsV2:log-groups/log-group/{encoded_log_group}'
            f'/log-events/{encoded_log_stream}'
            f'?filterPattern={filter_pattern}'
        )
