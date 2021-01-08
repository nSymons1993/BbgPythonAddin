import logging
import os

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(THIS_FOLDER, 'bbgLogFile.log')

with open(log_file, "w+") as f:
    f.seek(0)
    f.truncate()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler.setFormatter(file_formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)
stream_formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
stream_handler.setFormatter(stream_formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
