"""
##
# @file env_config.py
# @Synopsis  config environment
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-07
"""
import os
import datetime as dt

class EnvConfig(object):
    """
    # @Synopsis  config environment
    """
    PROJECT_NAME = 'long2short-rec'
    DEBUG = True
    PURE_TEXT_SIM = False
    SMS_RECEIVERS = ['18612861842']
    MAIL_RECEIVERS = ['guming@itv.baidu.com']
    # tv corresponds to tvplay in some cases, to tv itself in others...wtf, and
    # there is not such a word as 'tvplay', should be 'tvshow', I use 'tv' as the
    # only key correspond to the meaning of tvshow in this project
    LONG_VIDEO_TYPE_ALIAS_DICT = dict({
        'movie': 'movie',
        'tv': 'tvplay',
        'comic': 'comic',
        'show': 'show',
        })

    LONG_VIDEO_TYPE_NUM_DICT = dict({
        'movie': 0,
        'tv': 1,
        'show': 2,
        'comic': 3,
        })


    PLATFORM_LIST = ['PC', 'Mobile']
    PC_LOG_TYPE_LIST = ['play', 'view']
    MOBILE_LOG_TYPE_LIST = ['play', 'browse']
    VIDEO_TYPE_LIST = LONG_VIDEO_TYPE_ALIAS_DICT.keys() + ['short']

    PLATFORM_LIST = ['PC', 'Mobile']
    PC_LOG_TYPE_LIST = ['play', 'view']
    MOBILE_LOG_TYPE_LIST = ['play', 'browse']

    #find project root path according to the path of this config file
    CONF_PATH = os.path.split(os.path.realpath(__file__))[0]
    # if the 'conf' module is provided to spark-submit script in a .zip file,
    # the real path of this file would be project_path/conf.zip/conf(refer to
    # the dal.spark_submit module), while the
    # real path of config file we wanna locate is project_path/conf, thus the
    # following transformation would be neccessary.
    if '.zip' in CONF_PATH:
        path_stack = CONF_PATH.split('/')
        CONF_PATH = '/'.join(path_stack[:-2]) + '/conf'
    PROJECT_PATH = os.path.join(CONF_PATH, '../')
    LOG_PATH = os.path.join(PROJECT_PATH, 'log')
    GENERAL_LOG_PATH = os.path.join(LOG_PATH, 'general.log')
    #script path

    #tool path
    if DEBUG:
        TOOL_PATH = '/home/video/guming02/tools/'
    else:
        TOOL_PATH = '/home/video/guming/tools'
    HADOOP_PATH = os.path.join(TOOL_PATH, 'hadoop-client')
    HADOOP_JAVA_HOME = os.path.join(HADOOP_PATH, 'java6')
    HADOOP_BIN = os.path.join(HADOOP_PATH, 'hadoop/bin/hadoop')
    SPARK_SUBMIT_BIN = os.path.join(TOOL_PATH, 'spark-client/bin/spark-submit')
    SPARK_GBK_JAR_PATH = os.path.join(TOOL_PATH, 'spark-client/gbk.jar')
    VOR_CLIENT_PATH = os.path.join(TOOL_PATH, 'vor_client')
    LIB_SIGN_EXT_SO_PATH = os.path.join(TOOL_PATH, 'libsign_ext.so')
    MYSQL_BIN = 'mysql'
    MOLA_PATH = os.path.join(TOOL_PATH, 'mola')
    SMS_BIN = 'gsmsend'
    WORD_DICT_PATH = os.path.join(TOOL_PATH, 'dict')

    #HDFS input and output path
    HDFS_ROOT_PATH = "/app/vs/ns-video/"
    #user behavior log path
    HDFS_LOG_ROOT_PATH = dict()
    HDFS_LOG_ROOT_PATH['PC'] = os.path.join(HDFS_ROOT_PATH,
            'video-pc-data/vd-pc/behavior2/')
    HDFS_LOG_ROOT_PATH['Mobile'] = os.path.join(HDFS_ROOT_PATH,
            'video-mobile-data/vd-mobile/android-behavior-rec/')
    HDFS_LOG_PATH_DICT = dict()
    for platform in PLATFORM_LIST:
        HDFS_LOG_PATH_DICT[platform] = dict()
    # The log of PC play and view are saperated by work_type, which is not the
    # case in Mobile logs
    for log_type in PC_LOG_TYPE_LIST:
        HDFS_LOG_PATH_DICT['PC'][log_type] = dict()
        for video_type in VIDEO_TYPE_LIST:
            HDFS_LOG_PATH_DICT['PC'][log_type][video_type] = os.path.join(
                    HDFS_LOG_ROOT_PATH['PC'],
                    '{0}/{1}/'.format(log_type,
                        LONG_VIDEO_TYPE_ALIAS_DICT.get(video_type, video_type)))
    for log_type in MOBILE_LOG_TYPE_LIST:
        HDFS_LOG_PATH_DICT['Mobile'][log_type] = os.path.join(
                HDFS_LOG_ROOT_PATH['Mobile'], log_type)

    #short video meta path
    HDFS_SHORT_VIDEO_META_PATH = os.path.join(HDFS_ROOT_PATH, 'video-pc-data/vd-raw')

    #derivant output path
    HDFS_DERIVANT_PATH = os.path.join(HDFS_ROOT_PATH,
            'guming/{0}/'.format(PROJECT_NAME))

    #final output path
    HDFS_FINAL_PATH = os.path.join(HDFS_ROOT_PATH, \
            'video-pc-result/vr-pc-play-daily-trackpush-spark/%s/' % \
            dt.date.today().strftime('%Y%m%d'))

    #local path
    DATA_TYPE_LIST = ['source', 'intermediate', 'result']
    LOCAL_DATA_BASE_PATH = os.path.join(PROJECT_PATH, 'data')
    LOCAL_SHORT_VIDEO_META_PATH = os.path.join(LOCAL_DATA_BASE_PATH, 'short', 'source',
            'candidates')
    LOCAL_PGC_SHORT_VIDEO_META_PATH = os.path.join(LOCAL_DATA_BASE_PATH, 'short', 'source',
            'pgc_candidates')
    LOCAL_DATA_PATH_DICT = dict()
    for work_type in VIDEO_TYPE_LIST:
        LOCAL_DATA_PATH_DICT[work_type] = dict()
        for data_type in DATA_TYPE_LIST:
            LOCAL_DATA_PATH_DICT[work_type][data_type] = dict()
        for source_name in ['video_final']:
            LOCAL_DATA_PATH_DICT[work_type]['source'][source_name] =\
                    os.path.join(LOCAL_DATA_BASE_PATH, work_type, 'source',
                            source_name)
        for inter_name in ['long2short_sim', 'uv_rank', 'rowid', 'short_uv_rank',
                'itemcf_rec_list', 'content_rec_list', 'trunks']:
            LOCAL_DATA_PATH_DICT[work_type]['intermediate'][inter_name] =\
                    os.path.join(LOCAL_DATA_BASE_PATH, work_type, 'intermediate',
                            inter_name)
        for result_name in ['rec_list', 'readable_rec_list']:
            LOCAL_DATA_PATH_DICT[work_type]['result'][result_name] =\
                    os.path.join(LOCAL_DATA_BASE_PATH, work_type, 'result',
                            result_name)

    LOCAL_DATA_PATH_DICT['short']['intermediate']['colid'] =\
            os.path.join(LOCAL_DATA_BASE_PATH, 'short', 'intermediate',
                    'colid.pkl')

    LOG_NAME = PROJECT_NAME

if __name__ == '__main__':
    print EnvConfig.HDFS_SHORT_VIDEO_META_PATH
