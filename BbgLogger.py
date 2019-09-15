import logging

with open("bbgLogFile.log", "r+") as f:
    f.seek(0)
    f.truncate()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('bbgLogFile.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler.setFormatter(file_formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)
stream_formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
stream_handler.setFormatter(stream_formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)