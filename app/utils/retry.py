from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def network_retry(max_attempts: int = 3):
    return retry(stop=stop_after_attempt(max_attempts), wait=wait_exponential(multiplier=1, min=1, max=10))
