"""
# @file spark_submit.py
# @Synopsis  submit task to spark cluster
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-17
"""

import os
import commands
import sys
sys.path.append('..')
import logging

from conf.env_config import EnvConfig

logger = logging.getLogger(EnvConfig.LOG_NAME)

class SparkSubmit(object):
    """
    # @Synopsis  pack spark-submit into a python module
    """
    __TOP_MODULES = ['conf', 'bll', 'dal']

    @staticmethod
    def sparkSubmit(main_program_path, run_locally=False):
        """
        # @Synopsis  spark-submit
        #
        # @Args main_program_path either absolute path or relative path to the
        # current working path
        # @Args run_locally --master local option of spark-submit script
        #
        # @Returns  succeeded or not
        """
        origin_working_path = os.path.abspath('.')
        main_program_abspath = os.path.abspath(main_program_path)
        main_program_dir = os.path.split(main_program_abspath)[0]
        os.chdir(EnvConfig.PROJECT_PATH)
        for module_name in SparkSubmit.__TOP_MODULES:
            bash_cmd = 'zip -rq {0}.zip {0}/*'.format(module_name)
            status, output = commands.getstatusoutput(bash_cmd)
        os.chdir(main_program_dir)

        def zip_path_mapper(module_name):
            """
            # @Synopsis  zip required modules into .zip file, which is required by
            # spark-submit script
            #
            # @Args module_name
            #
            # @Returns nothing
            """
            zip_name = module_name + '.zip'
            zip_path = os.path.join(EnvConfig.PROJECT_PATH, zip_name)
            return zip_path

        module_zip_paths = map(zip_path_mapper, SparkSubmit.__TOP_MODULES)
        parameter_list = [EnvConfig.SPARK_SUBMIT_BIN]
        if run_locally:
            parameter_list.append('--master local')
        parameter_list.append('--py-files ' + ','.join(module_zip_paths))
        parameter_list.append('--jars ' + EnvConfig.SPARK_GBK_JAR_PATH)
        parameter_list.append(main_program_abspath)
        bash_cmd = ' '.join(parameter_list)
        status, output = commands.getstatusoutput(bash_cmd)
        logger.debug('Returned {0}: {1}\n{2}'.format(status, bash_cmd, output))
        os.chdir(origin_working_path)
        return status == 0

if __name__ == '__main__':
    SparkSubmit.sparkSubmit('../bll/tags_gen/tags_gen.py')
