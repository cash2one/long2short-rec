"""
# @file rdd_utils.py
# @Synopsis  rdd utils
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-06
"""
from conf.env_config import EnvConfig
from dal.hdfs import HDFS
import os

class RddUtils(object):
    """
    # @Synopsis  rdd utils
    """
    @staticmethod
    def saveRdd(rdd, hdfs_path, local_path):
        """
        # @Synopsis  save a rdd both to hdfs and local file system
        #
        # @Args rdd
        # @Args hdfs_path
        # @Args local_path
        #
        # @Returns  succeeded or not
        """

        succeeded = True
        if HDFS.exists(hdfs_path):
            succeeded = HDFS.rmr(hdfs_path)
        if succeeded:
            try:
                rdd.saveAsTextFile(hdfs_path)
            except Exception as e:
                print 'failed to save {0} to {1}'.format(
                    rdd, hdfs_path)
                print e.message
                return False
            if os.path.exists(local_path):
                print 'local path {0} exists, deleting it'\
                        .format(local_path)
                try:
                    os.remove(local_path)
                except Exception as e:
                    print 'failed to save {0} to {1}'.format(
                        hdfs_path, local_path)
                    return False
            succeeded = HDFS.getmerge(hdfs_path, local_path)
            return succeeded
        else:
            return False


    @staticmethod
    def savePickle(rdd, hdfs_path):
        """
        # @Synopsis  save a rdd as a pickle file to a hdfs url, if the path exits,
        # overwrite it
        #
        # @Args rdd
        # @Args hdfs_path
        #
        # @Returns   succeeded or not(to be done)
        """
        if HDFS.exists(hdfs_path):
            HDFS.rmr(hdfs_path)
        rdd.repartition(1).saveAsPickleFile(hdfs_path)


    @staticmethod
    def debugRdd(rdd, rdd_name, logger):
        """
        # @Synopsis  a debug function, send the infomation of a rdd to logger
        #
        # @Args rdd
        # @Args logger
        #
        # @Returns  nothing
        """
        logger.debug('{0}: {1}\t{2}'.format(rdd_name, rdd.count(),
            rdd.take(5)))

    @staticmethod
    def loadGBKFile(sc, input_path):
        """
        # @Synopsis  load gbk files from hdfs, which is not supported by orgin
        # spark textFile interface
        # @Args sc
        # @Args input_path
        # @Returns   rdd
        """
        ret_rdd = sc.hadoopFile(EnvConfig.HDFS_SHORT_VIDEO_META_PATH,
            'gbk.App', 'org.apache.hadoop.io.LongWritable',
            'org.apache.hadoop.io.Text').map(lambda x: x[1])
        return ret_rdd


