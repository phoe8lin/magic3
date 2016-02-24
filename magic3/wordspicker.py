# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import re
from builtins import compile as _compile
from mako.template import Template  # TODO: how about jinja2?

# used for find all english words
RE_ONLY_ENGLISH = re.compile("[A-Za-z]+")

# used for split sentence and pick chinese parts 
RE_NONE_CHINESE = re.compile("[\u0000-\u4DFF]|[\u9FA5-\uFFFF]")

# used for split sentence and pick chinese parts and english words
RE_NONE_CHINESE_ENGLISH = re.compile("[\u0000-\u002F]|[\u003A-\u0040]|[\u005B-\u0060]|[\u007B-\u4DFF]|[\u9FA5-\uFFFF]")

def pick_english(s:str):
    """ picking english words splited by other characters """
    return tuple(RE_ONLY_ENGLISH.findall(s))

def pick_chinese(s:str):
    """ like pick_english, but serve for chinese sentence """
    return tuple(w for w in RE_NONE_CHINESE.split(s) if w)
    
def pick_words(s:str):
    """ combined of `pick_english` and `pick_chinese` """
    return tuple(w for w in RE_NONE_CHINESE_ENGLISH.split(s) if w)

# source string of function `word_split`
# compile this in runtime for better performance
__word_split_codes = """
def word_split(input_string:str, charset:set)->tuple:
    buf = []
    def _extract():
        for c in input_string:
            if c in charset:
                buf.append(c)
            elif buf:
                yield ''.join(buf)
                buf.clear()
    return tuple(_extract())
"""

# check and compile `word_split`, for readability
# assign from globals dict again, it's useless
if 'word_split' not in globals():
    __byte_codes = _compile(__word_split_codes, filename='', mode='exec', optimize=2)
    exec(__byte_codes)
word_split = globals()['word_split']

def create_english_charset():
    """ return a set contains english chars """
    uppers = set(chr(i) for i in range(ord('a'),ord('z')))
    lowers = set(chr(i) for i in range(ord('A'),ord('Z')))
    return uppers | lowers

def create_chinese_charset():
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

def create_all_charset():
    """ combined of above """
    en_chr = create_english_charset()
    zh_chr = create_chinese_charset()
    all_chr = en_chr | zh_chr
    return all_chr


def test():
    """ simple test for this module """
    rawstr = "哔哩！哔哩？Ha+ha! ☆ 弹幕视频网 x（╯□╰）x 乾杯~ … china-bilibili@acg.net 莪咏逺嗳伱..."
    print(pick_english(rawstr))
    print(pick_chinese(rawstr))
    print(pick_words(rawstr))
    print(word_split(rawstr, create_english_charset()))
    print(word_split(rawstr, create_chinese_charset()))
    print(word_split(rawstr, create_all_charset()))

if __name__ == '__main__':
    test()

