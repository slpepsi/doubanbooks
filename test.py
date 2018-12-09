# -*- coding: utf-8 -*-
import re

str1='fdf"ds"f12\'3'
str2=re.sub('["\']','\\"',str1)
print str2
