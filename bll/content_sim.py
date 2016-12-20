"""
# @file content_sim.py
# @Synopsis  calculate content similarity between long video and short video,
# in this version, the similarity is 1 if the long video's trunk is a substring
# of short video's title, 0 otherwise
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-06-29
"""
from bll.data.video_info_util import VideoInfoUtil
from bll.data.data_source_conf import long_videos_dict
from conf.env_config import EnvConfig
from bll.utils.tokenizer import Tokenizer
import logging
import os

logger = logging.getLogger(EnvConfig.LOG_NAME)
def calContentSim(work_type, short_video_list):
    """
    # @Synopsis  calculate list of short videos that are related to each long video
    # in content.
    #
    # @Args work_type
    # @Args short_video_list candidate short video list
    #
    # @Returns   a dict {work_id : [related_short_vidoes]}
    """
    trunk2id_dict = VideoInfoUtil.getLongVideoTrunk2IdDict(work_type)

    term_short_videos_dict = dict()
    genLongVideoTrunkFile(work_type)
    user_dict_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'long_video_trunks')
    tokenizer = Tokenizer(user_dict_path=user_dict_path)

    processed_short_video_cnt = 0
    short_video_cnt = len(short_video_list)
    for short_video in short_video_list:
        # logger.debug('processed {0}/{1} videos'.format(processed_short_video_cnt, short_video_cnt))
        words = tokenizer.cutReservingTitleMark(short_video['title'])
        words = set(words)
        for word in words:
            if word not in term_short_videos_dict:
                term_short_videos_dict[word] = list()
            term_short_videos_dict[word].append(short_video)
        processed_short_video_cnt += 1


    long2short_sim_dict = dict()
    processed_trunk_cnt = 0
    trunk_cnt = len(trunk2id_dict)
    for trunk, vid_list in trunk2id_dict.iteritems():
        # logger.debug('processed {0}/{1} tunks'.format(processed_trunk_cnt, trunk_cnt))
        if trunk in term_short_videos_dict:
            related_short_video_list = term_short_videos_dict[trunk]
            related_short_video_list.sort(key=lambda x: x['play_cnt'], reverse=True)
            for vid in vid_list:
                long2short_sim_dict[vid] = related_short_video_list
        processed_trunk_cnt += 1
    return long2short_sim_dict


def genLongVideoTrunkFile(work_type):
    """
    # @Synopsis  save long video trunk to a file, in format work_id \t trunk \n
    # @Args work_type
    # @Returns   nothing
    """
    video_list = long_videos_dict[work_type].load()
    trunk_list = map(lambda x: x['trunk'].strip(), video_list)
    trunk_list = filter(lambda x: x != '', trunk_list)

    output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'long_video_trunks')
    output_obj = open(output_path, 'w')
    output_obj.write(u'\n'.join(trunk_list).encode('utf8'))
    output_obj.close()


if __name__ == '__main__':
    title_filter('tv')
