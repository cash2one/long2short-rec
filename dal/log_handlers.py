"""
# @file log_handlers.py
# @Synopsis  log handler, send sms and/or mail
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-06
"""
import logging
from sms import SMS
from mail import Mail
from conf.env_config import EnvConfig

class SMSHandler(logging.Handler):
    """
    # @Synopsis  inherit logging.Handler to make a customized log handler to send
    # email, because I don't know how the provided logging.handlers.SMTPHandler
    # works with the linux system mail service
    """

    def emit(self, record):
        """
        # @Synopsis  override emit method, to deal with the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        SMS.sendMessage(EnvConfig.SMS_RECEIVERS, msg)


class MailHandler(logging.Handler):
    """
    # @Synopsis  customized handler, to email critical log
    """

    def emit(self, record):
        """
        # @Synopsis  override logging.Handler emit method, the action when receive
        # the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        Mail.sendMail(EnvConfig.MAIL_RECEIVERS, 'PROGRAM ALARM', msg)
