"""
# @file candidate_select.py
# @Synopsis  select candidates of short videos in recommendation, since the
# total amount of short video is too large for calculation
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-06-29
"""
import os
import logging
import pyspark as ps
import pickle
from urllib import unquote
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.utils.log_file_name_gen import LogFileNameGen
from bll.utils.log_parser import LogParser
from bll.utils.rdd_utils import RddUtils
from bll.data.video_info_util import VideoInfoUtil
from dal.hdfs import HDFS

def main():
    """
    # @Synopsis  get short video candidates
    #
    # @Returns   nothing
    """
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    sc = ps.SparkContext()
    log_day_cnt = 28
    MAX_USER_SHORT_VIDEO_PER_DAY = 50
    # MAX_SHORT_VIDEO_UV_PER_DAY = 1000
    MIN_SHORT_VIDEO_UV_PER_DAY = 1

    # short_video_play_log = LogFileNameGen.genOneHourSampleLog('short',
    #         'play', 'PC')
    short_video_play_log = LogFileNameGen.genLogList(log_day_cnt, 1, 'short',
            'play', 'PC')

    short_video_play_log_rdd = sc.textFile(short_video_play_log)\
            .map(LogParser.parseWithoutException)\
            .filter(lambda x: x is not None)\
            .filter(lambda x: 'uid' in x and 'url' in x and 'playType' in x)\
            .map(lambda x: (x['uid'], unquote(x['url'])))\
            .filter(lambda x: 'v.baidu.com' not in x[1])\
            .filter(lambda x: 'video.baidu.com' not in x[1])\
            .distinct()

    def filterTooActiveUser(log_rdd, max_item_cnt):
        """
        # @Synopsis  filter users who watches too many videos(long or short) during
        # the time window, these users are probobaly robots
        #
        # @Args log_rdd
        # @Args max_item_cnt max number of videos a user should watch
        #
        # @Returns   filtered rdd
        """
        user_item_cnt = log_rdd\
                .map(lambda x: (x[0], 1))\
                .reduceByKey(lambda a, b: a + b)\
                .filter(lambda x: x[1] <= max_item_cnt)
        filtered_rdd = log_rdd\
                .join(user_item_cnt)\
                .filter(lambda x: x[1][1] is not None)\
                .map(lambda x: (x[0], x[1][0]))
        return filtered_rdd

    short_video_play_log_rdd = filterTooActiveUser(short_video_play_log_rdd,
            MAX_USER_SHORT_VIDEO_PER_DAY * log_day_cnt).cache()
    RddUtils.debugRdd(short_video_play_log_rdd, 'short_video_play_log_rdd', logger)

    short_video_uv_rdd = short_video_play_log_rdd\
            .map(lambda x: (x[1], 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .filter(lambda x: x[1] >= MIN_SHORT_VIDEO_UV_PER_DAY * log_day_cnt)

    short_video_meta_rdd = RddUtils.loadGBKFile(sc, EnvConfig.HDFS_SHORT_VIDEO_META_PATH)\
            .map(VideoInfoUtil.parseShortVideoMeta)\
            .filter(lambda x: x is not None)\
            .filter(lambda x: x['duration'] < 20 * 60)\
            .map(lambda x: (x.pop('url', None), x))
    # RddUtils.debugRdd(short_video_title_rdd, 'short_video_title_rdd', logger)

    def output_mapper(x):
        """
        # @Synopsis  output_mapper
        # @Args x
        # @Returns   output string in utf8
        """
        return u'\t'.join([str(x[0]), x[1][1]['title'], x[1][1]['image_link'],
                x[1][1]['duration'], str(x[1][0])]).encode('utf8')
    candidate_video_meta_rdd = short_video_uv_rdd\
            .join(short_video_meta_rdd)\
            .map(output_mapper)
    # RddUtils.debugRdd(candidate_video_meta_rdd, 'candidate_video_meta_rdd', logger)

    hdfs_path = os.path.join(EnvConfig.HDFS_DERIVANT_PATH, 'candidates')
    local_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'source', 'candidates')
    RddUtils.saveRdd(candidate_video_meta_rdd, hdfs_path, local_path)

if __name__ == '__main__':
    main()
