class LambdaContext:
    """The LambdaContext static object can be used to ease the development by providing the IDE type hints."""

    _function_name: str
    _function_version: str
    _invoked_function_arn: str
    _memory_limit_in_mb: int
    _aws_request_id: str
    _log_group_name: str
    _log_stream_name: str
    _identity: object
    _client_context: object

    @property
    def function_name(self) -> str:
        """The name of the Lambda function."""
        return self._function_name

    @property
    def function_version(self) -> str:
        """The version of the function."""
        return self._function_version

    @property
    def invoked_function_arn(self) -> str:
        """The Amazon Resource Name (ARN) that's used to invoke the function. Indicates if the invoker specified a
        version number or alias."""
        return self._invoked_function_arn

    @property
    def memory_limit_in_mb(self) -> int:
        """The amount of memory that's allocated for the function."""
        return self._memory_limit_in_mb

    @property
    def aws_request_id(self) -> str:
        """The identifier of the invocation request."""
        return self._aws_request_id

    @property
    def log_group_name(self) -> str:
        """The log group for the function."""
        return self._log_group_name

    @property
    def log_stream_name(self) -> str:
        """The log stream for the function instance."""
        return self._log_stream_name

    @staticmethod
    def get_remaining_time_in_millis() -> int:
        """Returns the number of milliseconds left before the execution times out."""
        return 0
