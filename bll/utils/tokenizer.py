#encoding: utf-8
"""
# @file tokenizer.py
# @Synopsis  tokenize text, using jieba
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""

import os
import sys
import jieba
import jieba.posseg as pseg
import logging
import datetime
import json
sys.path.append('../..')
from conf.env_config import EnvConfig

logger = logging.getLogger(EnvConfig.LOG_NAME)

class Tokenizer(object):
    """
    # @Synopsis   jieba tokenizer
    """
    def __init__(self, user_dict_path=None, stop_words_path=None):
        # jieba.enable_parallel(4)
        # jieba.initialize()
        self.stop_words = []
        if user_dict_path is not None:
            self.loadUserDict(user_dict_path)
        if stop_words_path is not None:
            self.loadStopWords(stop_words_path)

    def __del__(self):
        pass

    def tokenize(self, text):
        """
        # @Synopsis  tokenize string, with pos
        # @Args text
        # @Returns   [(word, pos)]
        """
        words = pseg.cut(text)
        ret = []
        for w in words:
            ret.append([w.word, w.flag])
        return ret

    def cut(self, text):
        """
        # @Synopsis  cut without pos
        # @Args text
        # @Returns   list of words
        """
        return jieba.cut(text, cut_all=False)

    def cutReservingTitleMark(self, text):
        """
        # @Synopsis  cut text, without touching substring between 《》
        # @Args text
        # @Returns   list of words
        """
        word_generator = self.cut(text)
        words = []
        for word in word_generator:
            words.append(word)
        ret = []
        i = 0
        while i < len(words):
            if words[i] == u'《':
                inside_words = [u'《']
                j = i + 1
                while j < len(words):
                    if words[j] == u'》':
                        if len(inside_words) > 0:
                            inside_words.append(u'》')
                            inside_content = u''.join(inside_words)
                            ret.append(inside_content)
                        #ret.append(u'》')
                        inside_words = []
                        i = j + 1
                        break
                    else:
                        inside_words.append(words[j])
                        j += 1
                ret += inside_words
                return ret
            else:
                ret.append(words[i])
                i += 1
        return ret

    def cutRemovingTitleMark(self, text):
        """
        # @Synopsis  cut text, without touching substring between 《》
        # @Args text
        # @Returns   list of words
        """
        word_generator = self.cut(text)
        words = []
        for word in word_generator:
            words.append(word)
        ret = []
        i = 0
        while i < len(words):
            ret.append(words[i])
            if words[i] == u'《':
                inside_words = []
                j = i + 1
                while j < len(words):
                    if words[j] == u'》':
                        if len(inside_words) > 0:
                            inside_content = u''.join(inside_words)
                            ret.append(inside_content)
                        ret.append(u'》')
                        inside_words = []
                        i = j + 1
                        break
                    else:
                        inside_words.append(words[j])
                        j += 1
                ret += inside_words
                return ret
            else:
                i += 1
        return ret


    def loadfromJson(self, json_text):
        """
        # @Synopsis  load  from json
        # @Args json_text
        # @Returns   [(word, pos)]
        """
        return json.loads(json_text)

    def loadAndFilter(self, json_text):
        """
        # @Synopsis  load and filter
        # @Args json_text
        # @Returns   [(word, pos)]
        """
        POS_WHITE_DICT = {
                'i': 'idiom',
                'j': '简称略语',
                'l': '习用语',
                'Ng': '名语素',
                'n': 'noun',
                'nr': '人名',
                'ns': '地名',
                'nt': '机构团体',
                'nz': '其他专名',
                's': 'space',
                # 'Tg': '时语素',
                # 't': '时间词',
                'Vg': '动语素',
                'v': '动词',
                'vd': '副动词',
                'vn': '名动词',
                'eng': 'English',
                }
        data_list = self.loadfromJson(json_text)
        data_list = filter(lambda x: x[1] in POS_WHITE_DICT, data_list)
        word_list = map(lambda x: x[0], data_list)
        word_list = filter(lambda x: len(x) >= 2, word_list)
        word_list = filter(lambda x: x not in self.stop_words, word_list)
        return word_list

    def loadStopWords(self, stop_words_path):
        """
        # @Synopsis  load stop words
        # @Args stop_words_path
        # @Returns   None
        """
        input_obj = open(stop_words_path)
        self.stop_words = map(lambda x: x.decode('utf8').strip('\n'), input_obj)

    def addStopWords(self, extra_stop_words):
        """
        # @Synopsis  add stop words
        # @Args extra_stop_words
        # @Returns   None
        """
        self.stop_words += extra_stop_words

    def loadUserDict(self, file_path):
        """
        # @Synopsis  load user dict
        # @Args file_path
        # @Returns   None
        """
        jieba.load_userdict(file_path)

    def addWord(self, word):
        """
        # @Synopsis  add single word
        # @Args word
        # @Returns   None
        """
        jieba.add_word(word)

if __name__ == "__main__":
    from conf.init_logger import InitLogger
    InitLogger()
    tokenizer = Tokenizer()
    tokenizer.loadUserDict('./test_dict.txt')
    # tokenizer.preprocess('tv', 'comments')
    tokenizer.addWord(u'邮差总按两次铃')
    tokenizer.addWord(u'邮差')

    test_str = u'仙剑云之凡，云之凡,邮差总按两次铃, 《仙剑云之凡》,《仙剑foo云之凡》 '
    word_list = tokenizer.cutReservingTitleMark(test_str)
    # word_list = tokenizer.cut(test_str)
    print u'/'.join(word_list).encode('utf8')

