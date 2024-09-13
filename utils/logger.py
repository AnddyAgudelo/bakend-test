import logging
import uuid

process_id = str(uuid.uuid4())


def logger_config(process_id_: str):
    logger_ = logging.getLogger('data_processor')
    logger_.setLevel(logging.INFO)

    log_file = "data_processor.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    logging_formatter = f"%(asctime)s {process_id_} %(levelname)s: %(message)s"
    file_formatter = logging.Formatter(logging_formatter)
    console_formatter = logging.Formatter(logging_formatter)
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    logger_.addHandler(file_handler)
    logger_.addHandler(console_handler)

    return logger_


logger = logger_config(process_id)
