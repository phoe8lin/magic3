#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2016.03.05
''' 
A group of constants for text tasks, contain regex and template 
'''
import re
from string import Template as PyTemplate
from functools import lru_cache

try:
    # from jinja2 import Template as JJTemplate
    # from mako.template import Template as MakoTemplate
    templateFactory = {
        'python' : PyTemplate,
        'Python' : PyTemplate,
        'default': PyTemplate,
        'jinja2' : JJTemplate,
        'Jinja2' : JJTemplate,
        'mako'   : MakoTemplate,
        'Mako'   : MakoTemplate
    }
except (ImportError, NameError):
    templateFactory = {
        'python' : PyTemplate,
        'Python' : PyTemplate,
        'default': PyTemplate
    }


def make_template(string, engine='python'):
    ''' make string to template object '''
    return templateFactory[engine](string)


# Frozenset for checking valid identifier bytes
identifierCharsetb = frozenset(b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                               b'abcdefghijklmnopqrstuvwxyz'
                               b'0123456789'
                               b'_.-')

# Frozenset for checking valid identifier chars
identifierCharset = frozenset('ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                              'abcdefghijklmnopqrstuvwxyz'
                              '0123456789'
                              '_.-')

# Used for find all english words
reOnlyEnglish = re.compile("[A-Za-z]+")

# Used for split sentence and pick chinese parts
reNoneChinese = re.compile("[\u0000-\u4DFF]|[\u9FA5-\uFFFF]")

# Used for split sentence and pick chinese parts and english words
reNoneChineseEnglish = re.compile("[\u0000-\u002F]|[\u003A-\u0040]|[\u005B-\u0060]|[\u007B-\u4DFF]|[\u9FA5-\uFFFF]")

# IPv4 address regex
reIPv4 = re.compile("^\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])$")

# Regex for match standard url on http, https and ftp protocal
reURL = re.compile("((http|ftp|https):"
                   "//[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)")

# Regex for match standard date
reDate = re.compile("(\\d{2}|\\d{4})[ -/]([0-2][1-9])[ -/]([0-3][0-9])", re.IGNORECASE)

# Regex for match standard time
reTime = re.compile("(\\d{1,2}):(\\d{2}):?(\\d{2})? ?(AM|PM)?", re.IGNORECASE)

# Regex for match telephone number
rePhone = re.compile("("
                     "(?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|"
                     "(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-]))"
                     ")")

# Regex for match standard email
reEmail = re.compile("([a-z0-9!#$%&'*+\/=?^_`{|.}~-]+@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)", re.IGNORECASE)

# Regex for match standard ipv6 address
reIPv6 = re.compile("\s*(?!.*::.*::)(?:(?!:)|:(?=:))"
                    "(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)){6}"
                    "("
                        "?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)[0-9a-f]{0,4}"
                        "(?:(?<=::)|(?<!:)|(?<=:)(?<!::):)|(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)"
                        "(?:\.(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)){3}"
                    ")\s*", re.VERBOSE | re.IGNORECASE | re.DOTALL)

# Not very useful...
rePrice = re.compile("[$]\s?[+-]?[0-9]{1,3}(?:(?:,?[0-9]{3}))*(?:\.[0-9]{1,2})?")

# File head of bash script file
bashHeader = '#!/bin/bash\n'

# File head of sh script file
shellHeader = '#!/bin/sh\n'

# File head of bash python3 file
python3Header = '#!/usr/bin/env python3\n# -*- coding:utf-8 -*-\n'

# FILE head of bash python2 file
python2Header = '#!/usr/bin/env python\n# -*- coding:utf-8 -*-\n'

# AWK command template
awkTemplate = make_template('''awk -F "${delim}" '{print ${vargs}}' ${files} 2>&1''')

# Key-Value template
kvTemplate = make_template('''${k},${v}\n''')

# Regex compiled cache
@lru_cache(maxsize=1000)
def compilex(sre, multi=False):
    if multi:
        return re.compile(sre, re.S | re.M)
    else:
        return re.compile(sre)

# constants for chinese_to_arabic
CN_NUM = {
    '〇' : 0, '一' : 1, '二' : 2, '三' : 3, '四' : 4, '五' : 5, '六' : 6, '七' : 7, '八' : 8, '九' : 9, '零' : 0,
    '壹' : 1, '贰' : 2, '叁' : 3, '肆' : 4, '伍' : 5, '陆' : 6, '柒' : 7, '捌' : 8, '玖' : 9, '貮' : 2, '两' : 2,
}
CN_UNIT = {
    '十' : 10,
    '拾' : 10,
    '百' : 100,
    '佰' : 100,
    '千' : 1000,
    '仟' : 1000,
    '万' : 10000,
    '萬' : 10000,
    '亿' : 100000000,
    '億' : 100000000,
    '兆' : 1000000000000,
}

def chinese_to_arabic(cn:str) -> int:
    unit = 0   # current
    ldig = []  # digest
    for cndig in reversed(cn):
        if cndig in CN_UNIT:
            unit = CN_UNIT.get(cndig)
            if unit == 10000 or unit == 100000000:
                ldig.append(unit)
                unit = 1
        else:
            dig = CN_NUM.get(cndig)
            if unit:
                dig *= unit
                unit = 0
            ldig.append(dig)
    if unit == 10:
        ldig.append(10)
    val, tmp = 0, 0
    for x in reversed(ldig):
        if x == 10000 or x == 100000000:
            val += tmp * x
            tmp = 0
        else:
            tmp += x
    val += tmp
    return val


# TODO: make a full unittest
def test():
    assert reOnlyEnglish.findall("cypro666@gmail.com") == ['cypro', 'gmail', 'com']
    assert reURL.match('https://github.com/cypro666/magic3/commit/7dcd')
    assert reDate.match('1995 12-03')
    assert reTime.match('22:53:04')
    assert reTime.match('05:55 PM')
    assert rePhone.match('13945678900')
    assert reEmail.match('cypro666@gmail.com')
    test_dig = ['八',
                '十一',
                '一百二十三',
                '一千二百零三',
                '一万一千一百零一',
                '十万零三千六百零九',
                '一百二十三万四千五百六十七',
                '一千一百二十三万四千五百六十七',
                '一亿一千一百二十三万四千五百六十七',
                '一百零二亿五千零一万零一千零三十八']
    for cn in test_dig:
        x = chinese_to_arabic(cn)
        print(cn, x)
    assert x == 10250011038


if __name__ == '__main__':
    test()




