"""
# @file hdfs.py
# @Synopsis  hadoop hdfs operations
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""


import commands
import sys
sys.path.append('..')
import os
from conf.env_config import EnvConfig
import logging

logger = logging.getLogger(EnvConfig.LOG_NAME)

class HDFS(object):
    """
    # @Synopsis  hadoop hdfs operations
    """
    @staticmethod
    def mkdir(hdfs_path):
        """
        # @Synopsis  mkdir
        """
        cmd = ' '.join(['-mkdir', hdfs_path])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def get(hdfs_path, local_path):
        """
        # @Synopsis  get single file from hdfs
        """
        cmd = ' '.join(['-get', hdfs_path, local_path])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def getmerge(hdfs_path, local_path):
        """
        # @Synopsis  get a director from hdfs and merge into one local file
        """
        cmd = ' '.join(['-getmerge', hdfs_path, local_path])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def put(local_path, hdfs_path):
        """
        # @Synopsis  put a local file to hdfs
        """
        cmd = ' '.join(['-put', local_path, hdfs_path])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def rmr(file_path):
        """
        # @Synopsis   remove a hdfs directory
        """
        cmd = ' '.join(['-rmr', file_path])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def exists(file_path):
        """
        # @Synopsis  test if a hdfs file/directory exists
        """
        cmd=' '.join(['-test -e', file_path])
        # cmd=' '.join(['-ls', 'hdfs://nj01-nanling-hdfs.dmop.baidu.com:54310/app'])
        return HDFS.hdfsCmdExec(cmd)


    @staticmethod
    def overwrite(local_path, hdfs_path):
        """
        # @Synopsis  put a local file to hdfs, if the hdfs path already exists,
        # overwrite it
        """
        if HDFS.exists(hdfs_path):
            HDFS.rmr(hdfs_path)
        succeeded = HDFS.put(local_path, hdfs_path)
        return succeeded

    @staticmethod
    def hdfsCmdExec(hdfs_cmd):
        """
        # @Synopsis  execute hadoop fs command
        #
        # @Args hdfs_cmd
        #
        # @Returns   the status of the execution
        """

        # in spark-1.4 client provided by Baidu, the environment variable
        # 'HADOOP_CONF_DIR' would be altered in the shell runing spark-submit
        # script, which means that environment variable would be inherited by
        # the subprocess called by the python program submitted to spark,
        # specifically, hadoop. This would cause the default hadoop
        # configuration to be overridden by that environment variable, and thus
        # doesn't work.
        # So what the following code doing is: store the current environment
        # variable, change that to the correct one, run hadoop, and then
        # restore it.
        origin_hadoop_conf_dir = os.environ.get('HADOOP_CONF_DIR', '')
        os.environ['HADOOP_CONF_DIR'] = os.path.join(EnvConfig.HADOOP_PATH,
                'hadoop/conf')
        sh_cmd = ' '.join([EnvConfig.HADOOP_BIN, 'fs', hdfs_cmd])
        status, output = commands.getstatusoutput(sh_cmd)
        logger.debug('Returned {0}: {1}\n{2}'.format(status, sh_cmd, output))
        os.environ['HADOOP_CONF_DIR'] = origin_hadoop_conf_dir
        return status == 0


if __name__ == '__main__':
    from conf.init_logger import InitLogger
    InitLogger()
    print HDFS.exists('/app')
    print HDFS.exists('/not_exist')
    print HDFS.exists('/app/vs/ns-video/video-pc-data/vd-pc/behavior2/play/tvplay/20160128')
