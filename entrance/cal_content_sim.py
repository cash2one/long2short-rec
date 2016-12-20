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
from bll.content_sim import calContentSim
from bll.content_sim import genLongVideoTrunkFile
import logging
import os
from datetime import datetime

if __name__ == '__main__':
    start_time = datetime.now()
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    calContentSim('tv')
    # genLongVideoTrunkFile('tv')
    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('cal_content_sim.py spent {0} minutes'.format(minutes))
