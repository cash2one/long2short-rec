"""
# @file merge_sim.py
# @Synopsis  merge the result of item-cf similarity, content similarity of long
# video and pgc short videos and content similarity of long video and regular
# short videos
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-06-29
"""
import json
import logging
from conf.env_config import EnvConfig
from data.data_source_conf import pgc_videos_table
from data.data_source_conf import long_videos_dict
from data.data_source_conf import short_videos_table
from bll.data.video_info_util import VideoInfoUtil
from bll.content_sim import calContentSim

logger = logging.getLogger(EnvConfig.LOG_NAME)

def getSimDict(file_path):
    """
    # @Synopsis  parse similar short video list from file, file format is
    # work_id \t url0::sim0$$url1::sim1 ...
    #
    # @Args file_path
    #
    # @Returns   dict({work_id: [(url, sim), ...]})
    """
    sim_dict = dict()
    input_obj = open(file_path)
    for line in input_obj:
        line = line.decode('utf8')
        fields = line.strip('\n').split('\t')
        vid = fields[0]
        url_sims_str = fields[1]
        if url_sims_str.strip() != '':
            try:
                url_sim_str_list = url_sims_str.split('$$')
            except Exception as e:
                url_sim_str_list = [url_sims_str]
            url_sim_list = map(lambda x: x.split('::'), url_sim_str_list)
            url_sim_list = map(lambda x: (x[0], float(x[1])), url_sim_list)
            url_sim_dict = dict(url_sim_list)
            sim_dict[vid] = url_sim_dict
    input_obj.close()
    return sim_dict


def calSim(work_type, max_sim_video_cnt=20):
    """
    # @Synopsis  merge item-cf and content similarity(both pgc and regular
    # short video). In this version, only pgc videos are involved
    #
    # @Args work_type
    # @Args max_sim_video_cnt max length of related short video list
    #
    # @Returns   nothing
    """
    # vid_itemcf_sim_dict = getSimDict(EnvConfig.LOCAL_DATA_PATH_DICT[
    #     work_type]['intermediate']['itemcf_rec_list'])
    # short_video_list = short_videos_table.load()
    # content_sim_dict = calContentSim(work_type, short_video_list)
    if EnvConfig.DEBUG:
        pgc_videos_table.update('backup')
        long_videos_dict[work_type].update('backup')
    else:
        pgc_videos_table.update('online')
        long_videos_dict[work_type].update('online')
    pgc_video_list = pgc_videos_table.load()
    pgc_sim_dict = calContentSim(work_type, pgc_video_list)

    merged_rec_list_dict = dict()
    vid_set = set(pgc_sim_dict)

    for vid in vid_set:
        rec_list = []
        related_pgc_list = pgc_sim_dict.get(vid, [])
        related_pgc_list.sort(key=lambda x: x['insert_time'], reverse=True)
        for video in related_pgc_list:
            if len(rec_list) < max_sim_video_cnt:
                video['is_pgc'] = True
                rec_list.append(video)
        '''
        content_sim_videos = content_sim_dict.get(vid, [])
        for url in vid_itemcf_sim_dict.get(vid, []):
            for video in content_sim_videos:
                if video['url'] == url:
                    video['is_pgc'] = False
                    rec_list.append(video)
                    break;
        '''

        MAX_COMMEN_SUBSTR_LEN = 15
        filtered_rec_list = []
        for video in rec_list:
            title_deduption = False
            for exist_video in filtered_rec_list:
                if len(longestSubstr(video['title'], exist_video['title'])) > MAX_COMMEN_SUBSTR_LEN:
                    title_deduption = True
                    break
            if not title_deduption:
                video['display_order'] = len(filtered_rec_list)
                filtered_rec_list.append(video)
        if len(filtered_rec_list) > 0:
            merged_rec_list_dict[vid] = filtered_rec_list

    merged_sim_path = EnvConfig.LOCAL_DATA_PATH_DICT[work_type]['result']['rec_list']
    output_obj = open(merged_sim_path, 'w')
    for vid, rec_list in merged_rec_list_dict.iteritems():
        output_obj.write(u'{0}\t{1}\n'.format(vid, json.dumps(rec_list, ensure_ascii=False))\
                .encode('utf8'))


def longestSubstr(string1, string2):
    """
    # @Synopsis  get the longest common substring of two string
    #
    # @Args string1
    # @Args string2
    #
    # @Returns   longest common substring
    """
    answer = ""
    len1, len2 = len(string1), len(string2)
    for i in range(len1):
        match = ""
        for j in range(len2):
            if (i + j < len1 and string1[i + j] == string2[j]):
                match += string2[j]
            else:
                if (len(match) > len(answer)): answer = match
                match = ""
    return answer
