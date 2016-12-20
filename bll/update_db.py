"""
# @file update_db.py
# @Synopsis  insert/update similar work result to mysql db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-22
"""
import sys
import os
sys.path.append('../..')
import time
import logging
import json
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.data.data_source_conf import lvideo_related_svideos_dict
from dal.mysql_conn import MySQLConn
from dal.image_extractor import ImageExtractor

logger = logging.getLogger(EnvConfig.LOG_NAME)

def videoIdentical(video0, video1):
    """
    # @Synopsis  compare value fields of two videos, return True if they are all
    # identical
    # @Args video0
    # @Args video1
    # @Returns   whether two videos are identical
    """
    key_list = ['title', 'image_link', 'duration']
    cmp_list = [video0[k] == video1[k] for k in key_list]
    return all(cmp_list)

class UpdateDb(object):

    @staticmethod
    def updateLVideoRelatedSvideo(work_type):
        """
        # @Synopsis  update mysql db similar_works table with calculated similar
        # work results
        # @Args works_type
        # @Returns   succeeded or not(to be done)
        """
        if EnvConfig.DEBUG:
            lvideo_related_svideos_dict[work_type].update('test')
        else:
            lvideo_related_svideos_dict[work_type].update('online')
        exist_videos = lvideo_related_svideos_dict[work_type].load()
        exist_relation_list = map(lambda x: ((x['work_id'], x['url']), x), exist_videos)
        exist_relation_dict = dict(exist_relation_list)

        new_relation_dict = dict()
        rec_list_file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH,
                work_type, 'result', 'rec_list')
        rec_list_obj = open(rec_list_file_path)
        for line in rec_list_obj:
            line = line.decode('utf8')
            work_id, json_str = line.strip('\n').split('\t')
            short_videos = json.loads(json_str)
            for video in short_videos:
                video['work_id'] = work_id
                new_relation_dict[(work_id, video['url'])] = video
        exist_keys = set(exist_relation_dict)
        new_keys = set(new_relation_dict)

        insert_keys = new_keys - exist_keys
        delete_keys = exist_keys - new_keys
        intersect_keys = new_keys & exist_keys
        update_keys = filter(lambda k: not videoIdentical(exist_relation_dict[k],
            new_relation_dict[k]), intersect_keys)
        update_dict = dict([(k, new_relation_dict[k]) for k in update_keys])

        logger.debug('insert: {0}, delete: {1}, update: {2}'.format(len(insert_keys),
                len(delete_keys), len(update_keys)))

        insert_video_list = [new_relation_dict[k] for k in insert_keys]
        def output_mapper(video):
            """
            # @Synopsis  map video to database required k-v format
            # @Args video
            # @Returns   formatted dict
            """
            out_dict = {
                    'work_id': video['work_id'],
                    'title': video['title'],
                    'link': video['url'],
                    'image_link': video['image_link'],
                    'duration': video['duration'],
                    'display_order': video['display_order'],
                    'is_pgc': video['is_pgc'],
                    # 'is_online': 1,
                    'channel_id': video['channel_id'],
                    'channel_title': video['channel_title'],
                    'channel_display_title': video['channel_display_title'],
                    'channel_image_link': video['channel_image_link']
                    }
            return out_dict
        insert_video_list = map(output_mapper, insert_video_list)
        update_dict = dict([(k, map(output_mapper, v)) for k, v in update_dict.iteritems()])
        table_name = '{0}_related_svideo'.format(work_type)

        if EnvConfig.DEBUG:
            conn = MySQLConn('ChannelPC', 'test')
        else:
            conn = MySQLConn('ChannelPC', 'online')

        n = conn.insert(table_name, insert_video_list)
        logger.debug('{0} lines inserted'.format(n))

        delete_keys = map(lambda x: {'work_id': x[0], 'link': x[1]}, delete_keys)
        n = conn.delete(table_name, list(delete_keys))
        logger.debug('{0} lines deleted'.format(n))

        n = conn.update(table_name, update_dict)
        logger.debug('{0} lines updated'.format(n))

        return True
