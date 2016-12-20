"""
# @file init_logger.py
# @Synopsis  init logger
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-19
"""
import sys
import logging
import logging.handlers
from conf.env_config import EnvConfig
from dal.log_handlers import MailHandler
from dal.log_handlers import SMSHandler

class InitLogger(object):
    """
    # @Synopsis  initiate logger
    """

    def __init__(self):
        logger = logging.getLogger(EnvConfig.LOG_NAME)
        logger.setLevel(logging.DEBUG)
        file_hdlr = logging.handlers.TimedRotatingFileHandler(
                EnvConfig.GENERAL_LOG_PATH, when='D', backupCount=7)
        stdout_hdler = logging.StreamHandler(sys.stdout)
        email_hdler =  MailHandler()
        email_hdler.setLevel(logging.ERROR)
        sms_handler = SMSHandler()
        sms_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s',
                "%Y-%m-%d %H:%M:%S")
        file_hdlr.setFormatter(formatter)
        stdout_hdler.setFormatter(formatter)
        email_hdler.setFormatter(formatter)
        sms_handler.setFormatter(formatter)
        logger.addHandler(file_hdlr)
        logger.addHandler(stdout_hdler)
        logger.addHandler(email_hdler)
        logger.addHandler(sms_handler)

if __name__ == '__main__':
    InitLogger()
