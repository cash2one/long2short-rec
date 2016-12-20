"""
# @file log_file_name_gen.py
# @Synopsis  generate log file paths(joined by comma) required by spark
# textFile function.
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-06
"""
import datetime as dt
from conf.env_config import EnvConfig
from dal.hdfs import HDFS

class LogFileNameGen(object):
    """
    # @Synopsis  generate log file path
    """
    @staticmethod
    def genLogList(day_cnt, days_before,
            work_type='movie', log_type='play', platform='PC'):

        """
        # @Synopsis  get hdfs log file list, to be the input to spark, already
        # filtered blank hdfs path which would cause error
        #
        # @Args day_cnt
        # @Args days_before how many days the time window is before today, e.g. if
        # you want to get the log up to yesterday, days_before is 1
        # @Args work_type
        # @Args platform
        #
        # @Returns  log path joined by ','
        # TODO: should add argument to determine which kind of log to get(play,
        # view, search, etc)
        """
        today = dt.date.today()
        deltas = range(days_before, days_before + day_cnt)
        days = [(today - dt.timedelta(i)) for i in deltas]
        time_window = map(lambda x: x.strftime('%Y%m%d'), days)

        if platform == 'PC':
            if log_type in ['play', 'view']:
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]\
                        [work_type]
            elif log_type == 'sim-work-click':
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['Mobile'][log_type]
        file_list = [(hdfs_path + date) for date in time_window]

        return ','.join(filter(lambda x: HDFS.exists(x), file_list))


    @staticmethod
    def genOneHourSampleLog(work_type='movie', log_type='play', platform='PC'):
        """
        # @Synopsis get a hourly play log file hdfs path, for test only
        #
        # @Args work_type
        # @Args log_type
        # @Args platform
        #
        # @Returns  hdfs path
        """
        sample_day = dt.date.today() - dt.timedelta(2)
        sample_day_str = sample_day.strftime('%Y%m%d')
        if platform == 'PC':
            if log_type in ['play', 'view']:
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]\
                        [work_type]
            elif log_type == 'sim-work-click':
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['Mobile'][log_type]

        log_file = '{0}{1}/{1}20'.format(hdfs_path, sample_day_str)
        return log_file
