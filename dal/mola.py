"""
@file mola.py
@Synopsis  import mola, for small data amount, save the trouble of specifying
 which database to insert into
@author Ming Gu(guming02@baidu.com))
@version 1.0
@date 2015-09-07
"""
from conf.env_config import EnvConfig
import os
import commands
import shutil
import logging
import re
import requests
from urllib import unquote
import json

logger = logging.getLogger(EnvConfig.LOG_NAME)
class Mola(object):
    """
    @Synopsis  mola operations, mainly insert(update)
    """
    __BIN_PATH = os.path.join(EnvConfig.MOLA_PATH, 'bin/update-db')
    __CONF_PATH = os.path.join(EnvConfig.MOLA_PATH, 'conf/update-db.conf')
    __TMP_CONF_PATH = os.path.join(EnvConfig.MOLA_PATH, 'conf/update-db.conf.tmp')

    URL = 'http://10.40.8.60:8083/vdo_db'
    TOKEN = 'ugcrp'
    MAX_HTTP_RETRY_CNT = 3

    @staticmethod
    def updateDb(key_prefix, file_path):
        """
        # @Synopsis  update mola operation
        #
        # @Args key_prefix the real key inserted into mola is the combination of
        # key_prefix and key in the file
        # @Args file_path shoud contain lines of 'key\tvalue'
        #
        # @Returns  succeeded or not
        """
        # the mola tool accept abspath rather than relative
        file_abspath = os.path.abspath(file_path)

        # we should insert into mola in both Beijing and Nanjing, since they
        # won't sync automatically
        mola_host_list = ['10.57.7.116', 'p00.nanjing.proxy.moladb.baidu.com']
        succeeded = True
        for host in mola_host_list:
            config_dict = dict({'prefix_key': key_prefix, 'IP': host})
            Mola.__alterConf(config_dict)
            bash_cmd = ' '.join([Mola.__BIN_PATH, '-t', file_abspath])
            # the update-db script relies on the working directory ., so we
            # have to cd that directory first
            full_cmd = 'cd {0} && {1}'.format(EnvConfig.MOLA_PATH, bash_cmd)
            status, output = commands.getstatusoutput(full_cmd)
            logger.debug('Returned {0}: {1}\n{2}'.format(
                status, full_cmd, output))
            if not status == 0:
                succeeded = False
                break
        return succeeded

    @staticmethod
    def __alterConf(config_dict):
        """
        # @Synopsis  change the value of one specified config entry, update the
        # config file
        #
        # @Args config_dict dict of {entry:value}
        #
        # @Returns  succeeded or not(to be done)
        """
        entries_to_modify = config_dict.keys()
        input_obj = open(Mola.__CONF_PATH, 'r')
        output_obj = open(Mola.__TMP_CONF_PATH, 'w')
        with input_obj, output_obj:
            for line in input_obj:
                line_modified = False
                for entry in entries_to_modify:
                    pattern = '{0} : .*$'.format(entry)
                    if re.match(pattern, line):
                        value = config_dict[entry]
                        output_obj.write('{0} : {1}\n'.format(entry, value))
                        line_modified = True
                        break
                if not line_modified:
                    output_obj.write(line)
        os.remove(Mola.__CONF_PATH)
        shutil.copy(Mola.__TMP_CONF_PATH, Mola.__CONF_PATH)


if __name__ == '__main__':
    # mola_dal = Mola()
    # mola_dal.updateDb('guming_test:', EnvConfig.LOCAL_CHANNEL_CONTENT_DICT['show'])
    key = 'http%3A%2F%2Fv.ku6.com%2Fshow%2FSjyUXvnFTnK884qAk7ecXQ...html'
    key = unquote(key)
    print Mola.getValue('video_short_meta', key).encode('utf8')
