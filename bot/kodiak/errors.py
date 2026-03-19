class RetryForSkippableChecks(Exception):
    pass


class PollForever(Exception):
    def __init__(self, reason: str = "") -> None:
        self.reason = reason


class ApiCallException(Exception):
    def __init__(self, method: str, http_status_code: int, response: bytes) -> None:
        self.method = method
        self.status_code = http_status_code
        self.response = response


class GitHubApiInternalServerError(Exception):
    pass
