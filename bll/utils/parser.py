"""
# @file parser.py
# @Synopsis  some parsers
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
from link_sign_utils import getImageLink
from conf.env_config import EnvConfig
import logging
logger = logging.getLogger(EnvConfig.LOG_NAME)
class LogParser(object):
    """
    # @Synopsis  log file parser
    """

    @staticmethod
    def parse(line):
        """
        # @Synopsis  parse method, throw exception if anything goes wrong
        #
        # @Args line
        #
        # @Returns   result dict
        """
        common_fields_dict = LogParser.getCommenFields(line)
        kv_dict = LogParser.loadDictFromStr(common_fields_dict['kv_field'])
        common_fields_dict.pop('kv_field', None)
        ret = dict(common_fields_dict, **kv_dict)
        return ret


    @staticmethod
    def parseWithoutException(line):
        """
        # @Synopsis  same to parse, except exceptions are handled by returing None
        #
        # @Args line
        #
        # @Returns   dict parsed
        """
        try:
            return LogParser.parse(line)
        except Exception as e:
            return None


    @staticmethod
    def getCommenFields(line):
        """
        # @Synopsis  get common fields in different type of logs
        #
        # @Args line
        #
        # @Returns   common fields dict
        """
        line = line.decode('utf8')
        fields = line.split('\t')
        if len(fields) != 5:
            raise ValueError('Insufficent fields in log line')
        uid = fields[0]
        if len(uid.strip()) != 32:
            raise ValueError('Invalid BaiduId')
        kv_field = fields[4]
        common_fields_dict = {'uid': uid, 'kv_field': kv_field}
        return common_fields_dict


    @staticmethod
    def loadDictFromStr(field):
        """
        # @Synopsis  deserialize dict from string
        #
        # @Args field
        #
        # @Returns   ret dict
        """
        result_dict = dict()
        try:
            kv_strs = field.split(';')
        except ValueError as e:
            kv_strs = [field]

        for kv_str in kv_strs:
            try:
                k, v = kv_str.split(':')
                result_dict[k] = v
            except ValueError as e:
                raise ValueError('K-V field format Error: {0}'.format(e.message))

        return result_dict


class RawShortVideoParser(object):
    """
    # @Synopsis  short video raw file parser
    """
    @staticmethod
    def parse(line):
        """
        # @Synopsis  parse method
        #
        # @Args line
        #
        # @Returns   result dict if line matches the supposed format, None
        # otherwise
        """
        try:
            fields = line.strip('\n').split('\t')
            url = fields[0].strip()
            title = fields[4].strip()
            image_sign1 = int(fields[8])
            image_sign2 = int(fields[9])
            duration = int(fields[10])
            insert_time = int(fields[14])
            if url != '' and title != '' and image_sign1 > 0 and\
                    image_sign2 > 0:
                tmp = (image_sign1 + image_sign2) % 3 + 1
                image_link = 'http://t{0}.baidu.com/it/u={1},{2}&fm=20'.format(
                        tmp, image_sign1, image_sign2)
                video_meta = dict({'url': url, 'title': title,
                    'image_link': image_link, 'duration': duration,
                    'insert_time': insert_time})
                return video_meta
        except Exception as e:
            pass


class VorOutputParser(object):
    """
    # @Synopsis  image extractor output parser
    """
    @staticmethod
    def parse(line):
        """
        # @Synopsis  parse method
        #
        # @Args line
        #
        # @Returns   dict({origin_image_link: image_link})
        """
        try:
            fields = line.strip('\n').split('\t')
            err_num = fields[7]
            if err_num == '0' or err_num == '2':
                origin_image_link = fields[0]
                image_link1 = int(fields[5])
                image_link2 = int(fields[6])
                image_link = getImageLink((image_link1, image_link2))
                return dict({'origin_image_link': origin_image_link,
                    'image_link': image_link})
            else:
                logger.critical('Image Extract Failed for line: {0}'\
                        .format(line.strip('\n')))

        except Exception as e:
            logger.critical('Image Extract Output Parser Failed on line: {0}, error_msg: {1}'\
                    .format(line.strip('\n'), e.message))

