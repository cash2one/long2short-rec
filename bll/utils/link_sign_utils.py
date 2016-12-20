"""
# @file link_sign_utils.py
# @Synopsis  compute hash signature of string
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-06
"""
import ctypes
from conf.env_config import EnvConfig

def getImageLink(image_sign):
    """
    # @Synopsis  get image link from link sign
    # @Args image_sign
    # @Returns   image link
    """
    tmp = (image_sign[0] + image_sign[1]) % 3 + 1
    image_link = 'http://t{0}.baidu.com/it/u={1},{2}&fm=20'.format(
            tmp, image_sign[0], image_sign[1])
    return image_link


def getLinkSign(src):
    """
    # @Synopsis  cal hash signature of string
    # @Args src
    # @Returns   (sign1, sign2)
    """
    g_sign_solib = ctypes.CDLL(EnvConfig.LIB_SIGN_EXT_SO_PATH)
    sign1 = ctypes.c_uint(0)
    sign2 = ctypes.c_uint(0)
    g_sign_solib.get_uln_sign_f64(ctypes.c_char_p(src),
	ctypes.c_uint64(len(src)), ctypes.byref(sign1), ctypes.byref(sign2))
    return (sign1.value, sign2.value)

