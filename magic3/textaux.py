#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2016.03.05
""" A group of constants for text tasks, contain regex and template """
import re
from string import Template as PyTemplate
try:
    from jinja2 import Template as JJTemplate
    from mako.template import Template as MakoTemplate
    templateFactory = {
        'python' : PyTemplate,
        'Python' : PyTemplate,
        'default': PyTemplate,
        'jinja2' : JJTemplate,
        'Jinja2' : JJTemplate,
        'mako'   : MakoTemplate,
        'Mako'   : MakoTemplate
    }
except ImportError:
    templateFactory = {
        'python' : PyTemplate,
        'Python' : PyTemplate,
        'default': PyTemplate
    }

def make_template(string, engine='python'):
    """ make string to template object """
    return templateFactory[engine](string)

# frozenset for checking valid identifier bytes
identifierCharsetb = frozenset(b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                               b'abcdefghijklmnopqrstuvwxyz'
                               b'0123456789'
                               b'_.-')

# frozenset for checking valid identifier chars
identifierCharset = frozenset('ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                              'abcdefghijklmnopqrstuvwxyz'
                              '0123456789'
                              '_.-')

# used for find all english words
reOnlyEnglish = re.compile("[A-Za-z]+")

# used for split sentence and pick chinese parts 
reNoneChinese = re.compile("[\u0000-\u4DFF]|[\u9FA5-\uFFFF]")

# used for split sentence and pick chinese parts and english words
reNoneChineseEnglish = re.compile("[\u0000-\u002F]|[\u003A-\u0040]|[\u005B-\u0060]|[\u007B-\u4DFF]|[\u9FA5-\uFFFF]")

# ipv4 address regex
reIPv4 = re.compile("^\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])$")

# regex for match standard url on http, https and ftp protocal
reURL = re.compile("((http|ftp|https):"
                   "//[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)")

# regex for match standard date 
reDate = re.compile("(\\d{2}|\\d{4})[ -][0-2][1-9][ -][0-3][0-9]", re.IGNORECASE)

# regex for match standard time
reTime = re.compile("\\d{1,2}:\\d{2} ?(?:[AP]\\.?M\\.?)?|\\d[AP]\\.?M\\.?|(:\\d{2})?", re.IGNORECASE)

# regex for match telephone number
rePhone = re.compile("("
                     "(?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|"
                     "(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-]))"
                     ")")

# regex for match standard email 
reEmail = re.compile("([a-z0-9!#$%&'*+\/=?^_`{|.}~-]+@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)", re.IGNORECASE)

# regex for match standard ipv6 address
reIPv6 = re.compile("\s*(?!.*::.*::)(?:(?!:)|:(?=:))"
                    "(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)){6}"
                    "("
                        "?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)[0-9a-f]{0,4}"
                        "(?:(?<=::)|(?<!:)|(?<=:)(?<!::):)|(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)"
                        "(?:\.(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)){3}"
                    ")\s*", re.VERBOSE|re.IGNORECASE|re.DOTALL)

# not very useful...
rePrice = re.compile("[$]\s?[+-]?[0-9]{1,3}(?:(?:,?[0-9]{3}))*(?:\.[0-9]{1,2})?")

# file head of bash script file
bashHeader = '#!/bin/bash\n'

# file head of sh script file
shellHeader = '#!/bin/sh\n'

# file head of bash python3 file
python3Header = '#!/usr/bin/env python3\n# -*- coding:utf-8 -*-\n'

# file head of bash python2 file
python2Header = '#!/usr/bin/env python\n# -*- coding:utf-8 -*-\n'

# awk command template
awkTemplate = make_template("""awk -F "${delim}" '{print ${vargs}}' ${files} 2>&1""")

# key-value template
kvTemplate = make_template("""${k},${v}\n""")


# TODO: make a full unittest
def test():
    assert reOnlyEnglish.findall("cypro666@gmail.com") == ['cypro', 'gmail', 'com']
    assert reURL.match('https://github.com/cypro666/magic3/commit/7dcd')
    assert reDate.match('1995 12-03')
    assert reTime.match('22:53:04')
    assert reTime.match('05:55 PM')
    assert rePhone.match('13945678900')
    assert reEmail.match('cypro666@gmail.com')
    
if __name__ == '__main__':
    test()



