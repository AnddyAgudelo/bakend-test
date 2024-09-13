from functools import wraps

import pandas as pd

from utils.logger import logger


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except FileNotFoundError as e:
            logger.error(f"File not found - Error: {e}")
        except pd.errors.EmptyDataError:
            logger.error(f"The file is empty - Error: {args[0]}")
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing file - Error: {e}")
        except ValueError as e:
            logger.error(f"Value error: Error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: Error: {e}")

    return wrapper
