import os
import bugsnag
from bugsnag.handlers import BugsnagHandler
import logging

# Configure Bugsnag with your API key
bugsnag.configure(
    api_key=os.getenv("BUGSNAG_KEY"),
    project_root='/',
    release_stage=os.getenv("APP_ENV")
)

# Set up logging to Bugsnag
logger = logging.getLogger("BUGSNAG")
handler = BugsnagHandler()
# Set the log level to ERROR to report errors to Bugsnag
handler.setLevel(logging.ERROR)
logger.addHandler(handler)


def notify_bugsnag(error):
    error_str = str(error)
    if isinstance(error, str):
        error = Exception(error)
    bugsnag.notify(error)
    logger.error(f" {error_str}", exc_info=True)


if __name__ == "__main__":
    notify_bugsnag("Test Bugsnag Event")
