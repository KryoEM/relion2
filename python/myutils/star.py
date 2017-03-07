#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 16:01:52 2016

@author: Oleg Kuybeda
"""

def getlist(starfile,key):
    ''' returns a list of strings that follows key in startfile '''
    with open(starfile) as f:
        content = f.readlines()
        
    tf   = [l.find(key)==0 for l in content]
    idxs = [i for i, x in enumerate(tf) if x]
    if len(idxs) == 0:
        raise ValueError("Key %s not found in star file %s" % (key,starfile))
    items = [content[i].strip() for i in range(idxs[0]+1,len(content))]
    items = [c for c in items if len(c)>0]
    return items
