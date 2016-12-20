"""
# @file sms.py
# @Synopsis  sms service, used to send short message to corresponding peaple
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-07
"""
from conf.env_config import EnvConfig
import commands
import socket
import getpass
import logging

logger = logging.getLogger(EnvConfig.LOG_NAME)

class SMS(object):
    """
    # @Synopsis  short message service
    """

    def __init__(self):
        pass

    @staticmethod
    def sendMessage(phone_numbers, message):
        """
        # @Synopsis  send message
        #
        # @Args a list of phone numbers
        # @Args message
        #
        # @Returns   succeeded or not
        """
        host_name = socket.gethostname()
        user_name = getpass.getuser()
        sms_server = 'emp01.baidu.com:15002'
        backup_server = 'emp02.baidu.com:15002'

        out_msg = '{0}@{1} : {2}'.format(user_name, host_name, message)
        for phone_num in phone_numbers:
            bash_cmd = '{0} -s {1} {2}@"{3}"'.format(EnvConfig.SMS_BIN,
                    sms_server, phone_num, out_msg)
            status, output = commands.getstatusoutput(bash_cmd)
            logger.debug('Returned {0}: {1}\n{2}'.format(
                status, bash_cmd, output))

if __name__ == '__main__':
    SMS.sendMessage(['18612861842'], 'test message')
