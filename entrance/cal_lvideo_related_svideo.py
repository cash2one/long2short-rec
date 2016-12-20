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
from bll.cal_sim import calSim
from bll.update_db import UpdateDb
import logging
import os
from datetime import datetime

if __name__ == '__main__':
    start_time = datetime.now()
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    work_type = sys.argv[1]

    calSim(work_type)
    UpdateDb.updateLVideoRelatedSvideo(work_type)

    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('cal_lvideo_related_svideo spent {0} minutes'.format(minutes))
