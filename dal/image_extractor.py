"""
# @file image_extractor.py
# @Synopsis  extract image, the vor_client in tool directory crops the original
# image and and generate a image with specified size(in vor_client's conf dir)
# and return a baidu own url on which the extract image stored
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
import os
import commands
import logging
from conf.env_config import EnvConfig
from bll.utils.link_sign_utils import getLinkSign
from bll.utils.parser import VorOutputParser
logger = logging.getLogger(EnvConfig.LOG_NAME)

class ImageExtractor(object):
    """
    # @Synopsis  extract image
    """
    def __init__(self):
        """
        # @Synopsis  init
        #
        # @Returns   None
        """
        self.input_file_path = os.path.join(EnvConfig.VOR_CLIENT_PATH,
                'data', 'input', 'img_link.txt')
        self.output_file_path = os.path.join(EnvConfig.VOR_CLIENT_PATH,
                'data', 'output', 'output.txt')
        self.bin_path = os.path.join(EnvConfig.VOR_CLIENT_PATH, 'bin', 'task_sender')

    def extract(self, video_list):
        """
        # @Synopsis  extract image for the given video list, which contains videos
        # with 'origin_image_link' field, return the origin link with addtion of
        # field 'image_link' for every row succeed in image extraction
        #
        # @Args video_list
        #
        # @Returns   altered video_list, with 'image_link' field
        """
        def calLinkSignMapper(video):
            """
            # @Synopsis  cal link sign for each video, which is needed for image
            # extraction client
            #
            # @Args video
            #
            # @Returns   video with additional 'url' field
            """
            link_sign =  getLinkSign(video['url'])
            video['link_sign'] = link_sign
            return video
        video_list = map(calLinkSignMapper, video_list)
        '''
        input_file_obj = open(self.input_file_path, 'w')
        for video in video_list:
            fields = [video['origin_image_link'], '0', '0',
                    str(video['link_sign'][0]), str(video['link_sign'][1]),
                    '5', '1', '0']
            input_file_obj.write('\t'.join(fields) + '\n')
        input_file_obj.close()
        logger.debug('image extractor input file generated')

        pwd = os.getcwd()
        bash_cmd = 'cd {0} && ./bin/task_sender && cd {1}'.format(
                EnvConfig.VOR_CLIENT_PATH, pwd)
        logger.debug('extracting image, waiting...')
        status, output = commands.getstatusoutput(bash_cmd)
        logger.debug('Returned {0}: {1}\n{2}'.format(
            status, bash_cmd, output))
        if not status == 0:
            logger.critical('extract image failed')
            return False
        '''
        output_obj = open(self.output_file_path)
        ret = map(VorOutputParser.parse, output_obj)
        ret = filter(lambda x: x is not None, ret)
        output_obj.close()

        ret = map(lambda x: (x['origin_image_link'], x['image_link']), ret)
        image_link_dict = dict(ret)
        for video in video_list:
            origin_image_link = video['origin_image_link']
            video['image_link'] = image_link_dict.get(origin_image_link, '')
        return video_list
