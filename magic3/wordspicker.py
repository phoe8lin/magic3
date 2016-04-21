# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import re
from builtins import compile as _compile
from mako.template import Template  # TODO: how about jinja2?

# used for find all english words
ReOnlyEnglish = re.compile("[A-Za-z]+")

# used for split sentence and pick chinese parts 
ReNoneChinese = re.compile("[\u0000-\u4DFF]|[\u9FA5-\uFFFF]")

# used for split sentence and pick chinese parts and english words
ReNoneChineseEnglish = re.compile("[\u0000-\u002F]|[\u003A-\u0040]|[\u005B-\u0060]|[\u007B-\u4DFF]|[\u9FA5-\uFFFF]")

def PickEnglish(s:str):
    """ picking english words splited by other characters """
    return tuple(ReOnlyEnglish.findall(s))

def PickChinese(s:str):
    """ like pickEnglish, but serve for chinese sentence """
    return tuple(w for w in ReNoneChinese.split(s) if w)
    
def PickWords(s:str):
    """ combined of `pickEnglish` and `pickChinese` """
    return tuple(w for w in ReNoneChineseEnglish.split(s) if w)

# source string of function `wordSplit`
# compile this in runtime for better performance
_WordSplitCodes = """
def WordSplit(inputstr:str, charset:set)->tuple:
    buf = []
    def _extract():
        for c in inputstr:
            if c in charset:
                buf.append(c)
            elif buf:
                yield ''.join(buf)
                buf.clear()
    return tuple(_extract())
"""

# check and compile `wordSplit`, for readability
# assign from globals dict again, it's useless
if 'WordSplit' not in globals():
    __ByteCodes = _compile(_WordSplitCodes, filename='', mode='exec', optimize=2)
    exec(__ByteCodes)
WordSplit = globals()['WordSplit']

def CreateEnglishCharset():
    """ return a set contains english chars """
    uppers = set(chr(i) for i in range(ord('a'),ord('z')))
    lowers = set(chr(i) for i in range(ord('A'),ord('Z')))
    return uppers | lowers

def CreateChineseCharset():
    """ return a set contains chinese chars in utf-8 """
    codet = Template("""
<% 
begin, end = 0x4E00, 0x9FA4
%>
set((
% for item in range(begin, end):
    ${prefix}${hex(item).lstrip('0x')}${suffix},
% endfor
    ${prefix}${hex(end).lstrip('0x')}${suffix}
))
""")
    src = codet.render(prefix='\"\\u', suffix='\"')
    return eval(compile(src, filename='', mode='eval', optimize=2))

def CreateAllCharset():
    """ combined of above """
    en_chr = CreateEnglishCharset()
    zh_chr = CreateChineseCharset()
    all_chr = en_chr | zh_chr
    return all_chr


def test():
    """ simple test for this module """
    rawstr = "哔哩！哔哩？Ha+ha! ☆ 弹幕视频网 x（╯□╰）x 乾杯~ … china-bilibili@acg.net 莪咏逺嗳伱..."
    print(PickEnglish(rawstr))
    print(PickChinese(rawstr))
    print(PickWords(rawstr))
    print(WordSplit(rawstr, CreateEnglishCharset()))
    print(WordSplit(rawstr, CreateChineseCharset()))
    print(WordSplit(rawstr, CreateAllCharset()))

if __name__ == '__main__':
    test()

