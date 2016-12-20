"""
# @file tags_gen.py
# @Synopsis  gen tags
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-20
"""

import sys
sys.path.append('..')
import commands
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from dal.spark_submit import SparkSubmit
from bll.gen_rec_list import genRecList
import logging
import os
from datetime import datetime

if __name__ == '__main__':
    start_time = datetime.now()
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    main_program_path = '../bll/item_cf.py'
    SparkSubmit.sparkSubmit(main_program_path, run_locally=False)
    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('cal_itemcf spent {0} minutes'.format(minutes))
