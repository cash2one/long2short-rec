"""
# @file item_cf.py
# @Synopsis  calculate itemcf-iuf similarity of movies and get video uv rank
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""

import sys
import os
import logging
import pyspark as ps
import pickle
import math
import heapq
from urllib import unquote
from scipy.sparse import csr_matrix
from sklearn.externals import joblib
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.utils.log_file_name_gen import LogFileNameGen
from bll.utils.log_parser import LogParser
from bll.utils.rdd_utils import RddUtils
from bll.data.video_info_util import VideoInfoUtil
from dal.hdfs import HDFS


def filterTooActiveUser(log_rdd, max_item_cnt):
    """
    # @Synopsis  filter too active users, who might be robot
    # @Args log_rdd
    # @Args max_item_cnt
    # @Returns   filtered log rdd
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


def rec_list_mapper(x):
    """
    # @Synopsis  map rdd line to output format
    # @Args x
    # @Returns   utf8 line
    """
    long_video_id = x[0]
    url_sim_list = x[1]
    url_sim_str_list = map(lambda a: u'{0}::{1:.4f}'.format(a[0], a[1]), url_sim_list)
    url_sims_str = u'$$'.join(url_sim_str_list)
    ret = u'{0}\t{1}'.format(long_video_id, url_sims_str).encode('utf8')
    return ret


def main():
    """
    # @Synopsis  calculate itemcf-iuf similarity
    # @Returns   nothing
    """
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    sc = ps.SparkContext()
    long_video_type = 'tv'
    log_day_cnt = 28
    MAX_SIM_VIDEO_CNT = 100
    MAX_USER_LONG_VIDEO_CNT_PER_DAY = 20
    MAX_USER_SHORT_VIDEO_PER_DAY = 50
    MAX_SHORT_VIDEO_UV_PER_DAY = 1000
    MIN_SHORT_VIDEO_UV_PER_DAY = 1

    long_video_play_log = LogFileNameGen.genLogList(log_day_cnt, 1, long_video_type, 'play', 'PC')
    short_video_play_log = LogFileNameGen.genLogList(log_day_cnt, 1, 'short', 'play', 'PC')
    long_video_play_log_rdd = sc.textFile(long_video_play_log)\
            .map(LogParser.parseWithoutException)\
            .filter(lambda x: x is not None)\
            .filter(lambda x: 'uid' in x and 'id' in x)\
            .map(lambda x: (x['uid'], x['id']))\
            .distinct()
    short_video_play_log_rdd = sc.textFile(short_video_play_log)\
            .map(LogParser.parseWithoutException)\
            .filter(lambda x: x is not None)\
            .filter(lambda x: 'uid' in x and 'url' in x and 'playType' in x)\
            .map(lambda x: (x['uid'], unquote(x['url'])))\
            .filter(lambda x: 'v.baidu.com' not in x[1])\
            .filter(lambda x: 'video.baidu.com' not in x[1])\
            .distinct()
    long_video_play_log_rdd = filterTooActiveUser(long_video_play_log_rdd,
            MAX_USER_LONG_VIDEO_CNT_PER_DAY * log_day_cnt).cache()
    short_video_play_log_rdd = filterTooActiveUser(short_video_play_log_rdd,
            MAX_USER_SHORT_VIDEO_PER_DAY * log_day_cnt).cache()
    long_video_uv_rdd = long_video_play_log_rdd\
            .map(lambda x: (x[1], 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .cache()
    short_video_uv_rdd = short_video_play_log_rdd\
            .map(lambda x: (x[1], 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .cache()
    short_video_uv_list = short_video_uv_rdd.collect()
    short_video_uv_list.sort(key=lambda x: x[1], reverse=True)
    short_video_uv_list = map(lambda x: u'\t'.join((x[0], str(x[1]))), short_video_uv_list)
    output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'intermediate',
            'short_uv_rank')
    output_obj = open(output_path, 'w')
    output_obj.write(u'\n'.join(short_video_uv_list).encode('utf8'))

    short_video_uv_rdd = short_video_uv_rdd\
            .filter(lambda x: x[1] <= MAX_SHORT_VIDEO_UV_PER_DAY * log_day_cnt)\
            .filter(lambda x: x[1] >= MIN_SHORT_VIDEO_UV_PER_DAY * log_day_cnt)\
            .cache()
    short_video_play_log_rdd = short_video_play_log_rdd\
            .map(lambda x: (x[1], x[0]))\
            .join(short_video_uv_rdd)\
            .filter(lambda x: x[1][1] is not None)\
            .map(lambda x: (x[1][0], x[0]))\
            .cache()
    short_video_meta_rdd = RddUtils.loadGBKFile(sc, EnvConfig.HDFS_SHORT_VIDEO_META_PATH)\
            .map(VideoInfoUtil.parseShortVideoMeta)\
            .filter(lambda x: x is not None)
    coplay_count_rdd = long_video_play_log_rdd\
            .join(short_video_play_log_rdd)\
            .map(lambda x: (x[1], 1))\
            .reduceByKey(lambda a, b: a + b)
    cos_sim_rdd = coplay_count_rdd\
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .join(long_video_uv_rdd) \
        .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))) \
        .join(short_video_uv_rdd) \
        .map(lambda x: ((x[1][0][0], x[0]), x[1][0][1], (x[1][0][2], x[1][1]))) \
        .map(lambda x: (x[0], x[1]/math.sqrt((x[2][0]) * (x[2][1]))))

    rec_list_rdd = cos_sim_rdd\
            .map(lambda x: (x[0][0], (x[0][1], x[1])))\
            .groupByKey()\
            .map(lambda x: (x[0], heapq.nlargest(MAX_SIM_VIDEO_CNT, x[1], key=lambda a: a[1])))\
            .map(rec_list_mapper)\
            .cache()
    RddUtils.debugRdd(rec_list_rdd, 'rec_list_rdd', logger)
    hdfs_path = os.path.join(EnvConfig.HDFS_DERIVANT_PATH, 'itemcf_rec_list')
    local_path = os.path.join(EnvConfig.LOCAL_DATA_PATH_DICT[long_video_type]\
            ['intermediate']['itemcf_rec_list'])
    RddUtils.saveRdd(rec_list_rdd, hdfs_path, local_path)

    vid_uv_list = long_video_uv_rdd.collect()
    vid_uv_list.sort(key=lambda x: x[1], reverse=True)
    logger.debug('len(vid_uv_list) is {0}'.format(len(vid_uv_list)))
    vid_uv_str_list = map(lambda x: '{0}\t{1}'.format(x[0], x[1]), vid_uv_list)
    output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, long_video_type, 'intermediate',
            'uv_rank')
    output_obj = open(output_path, 'w')
    with output_obj:
        output_obj.write('\n'.join(vid_uv_str_list))


if __name__ == '__main__':
    main()

