import logging

# Create a logger and set its level to DEBUG which will log all messages
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a handler for the logger to specify where to log the messages (console in this case)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

# Create a handler to specify where to log the messages (file in this case)
fileHandler = logging.FileHandler('/home/huymonkey/on-premise-data-pipeline/monitoring/logs/logging.txt')
fileHandler.setLevel(logging.INFO)

# Create a formatter to specify the format of the messages
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(format)
fileHandler.setFormatter(format)

# Add the handler to the logger
logger.addHandler(handler)
logger.addHandler(fileHandler)

logger.info("Started")