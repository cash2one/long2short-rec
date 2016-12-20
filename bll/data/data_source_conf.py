"""
# @file data_source_conf.py
# @Synopsis  contains configuration of data sources
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
import os
from conf.env_config import EnvConfig
from table_file import TableFile
from cached_mysql_table import CachedMySQLTable

__all__ = ['short_videos_table', 'pgc_videos_table', 'long_videos_dict',
        'lvideo_related_svideos_dict']

def field_list_mapper(x):
    """
    # @Synopsis  map field list to requried format
    # @Args x
    # @Returns   list of dicts
    """
    if len(x) == 2:
        return {'field_name': x[0], 'data_type': x[1]}
    elif len(x) == 3:
        return {'field_name': x[0], 'column_name': x[1], 'data_type': x[2]}


def alterFieldList(field_list, field_name, new_field_setting):
    """
    # @Synopsis  alter field list, for convience of configuring similar data
    # sources, etc. four types of long videos
    #
    # @Args field_list
    # @Args field_name
    # @Args new_field_setting
    #
    # @Returns   None
    """
    for field_num, field_setting in enumerate(field_list):
        if field_setting[0] == field_name:
            field_list[field_num] = new_field_setting

field_list = [
        ['url', unicode],
        ['title', unicode],
        ['image_link', unicode],
        ['duration', int],
        ['play_cnt', int],
        ]
field_list = map(field_list_mapper, field_list)
file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'source',
        'candidates')
short_videos_table = TableFile(field_list, file_path)

field_list = [
        ['url', 'recommend_video.play_link', unicode],
        ['title', 'recommend_video.title', unicode],
        ['image_link', 'recommend_video.image_link', unicode],
        ['duration', 'recommend_video.duration', unicode],
        ['play_cnt', 'recommend_video.play_num', int],
        ['insert_time', 'recommend_video.insert_time', int],
        ['channel_id', 'recommend_tag.id', int],
        ['channel_title', 'recommend_tag.title', unicode],
        ['channel_display_title', 'recommend_tag.display_title', unicode],
        ['channel_image_link', 'recommend_tag.imgurl', unicode]
        ]
field_list = map(field_list_mapper, field_list)
conditions = ['recommend_video.source_type=1', 'recommend_video.status=1',
        'recommend_video.id=album_video_rela.video_id',
        'album_video_rela.album_id=recommend_album.id',
        'recommend_album.tag_id=recommend_tag.id'
        ]
condition_str = ' and '.join(conditions)

file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short',
        'source', 'pgc_candidates')
table_name = 'recommend_video, recommend_tag, recommend_album, album_video_rela'
pgc_videos_table = CachedMySQLTable(table_name = table_name, field_list=field_list,
        condition_str=condition_str, db_name='ChannelPC', file_path=file_path)


lvideo_related_svideos_dict = dict()
field_list = [
        ['url', 'link', unicode],
        ['title', 'title', unicode],
        ['image_link', 'image_link', unicode],
        ['duration', 'duration', unicode],
        ['work_id', 'work_id', unicode],
        ['insert_time', 'insert_time', int],
        ]
field_list = map(field_list_mapper, field_list)
condition_str = '1'

for work_type in EnvConfig.LONG_VIDEO_TYPE_ALIAS_DICT:
    file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, work_type, 'source',
            'lvideo_related_svideo')
    lvideo_related_svideos_dict[work_type]= CachedMySQLTable(
            table_name = '{0}_related_svideo'.format(work_type),
            field_list=field_list, condition_str=condition_str, db_name='ChannelPC',
            file_path=file_path)

long_videos_dict = dict()
for work_type in EnvConfig.LONG_VIDEO_TYPE_ALIAS_DICT:
    field_list = [
            ['work_id', 'works_id', unicode],
            ['trunk', 'trunk', unicode],
            ['aliases', 'alias', unicode],
            ['directors', 'director', unicode],
            ['actors', 'actor', unicode],
            ['categories', 'type', unicode],
            ['tags', 'rc_tags', unicode],
            ['areas', 'area', unicode],
            ['release_year', 'al_date', int],
            ['available', 'all_sites!=""', bool],
            ['version', 'version', unicode],
            ['language', 'language', unicode],
            ['finish', 'finish', bool],
            ['author', '""', unicode],
            ['sites', 'all_sites', unicode]
            ]
    if work_type == 'movie':
        alterFieldList(field_list, 'release_year', ['release_year', 'net_show_time', int])
        alterFieldList(field_list, 'available', ['free', 'sites!=""', bool])
        alterFieldList(field_list, 'finish', ['finish', '1', bool])
        alterFieldList(field_list, 'sites', ['sites', 'sites', str])
    if work_type == 'comic':
        alterFieldList(field_list, 'author', ['author', 'author', str])
    if work_type == 'show':
        alterFieldList(field_list, 'directors', ['hosts', 'host', str])
    field_list = map(field_list_mapper, field_list)

    conditions = ['title!=""', 'big_poster!=""']
    if work_type == 'movie':
        conditions.append('source!=16')
    condition_str = ' and '.join(conditions)
    file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, work_type, 'source', 'video_final')
    long_videos_dict[work_type] = CachedMySQLTable(table_name = '{0}_final'.format(work_type),
            field_list=field_list, condition_str=condition_str, db_name='Final',
            file_path=file_path)

