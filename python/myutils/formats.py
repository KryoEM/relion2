#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 17:26:46 2016

@author: Oleg Kuybeda
"""

import myutils.filenames as fn
from   myutils.utils import sysrun #,tprint #Base,,
#from   os.path import basename #dirname
import multiprocessing
from   myutils import mrc
import numpy as np

def untbz(tbzfile,untbzdir,**kwargs):
    nth = kwargs.pop('nthreads',multiprocessing.cpu_count())
    ''' Uncompresses list of files from a tbz archive '''
    tarfile = fn.replace_ext(tbzfile, '.tar')
    # Unzip tbz
    cmd     = "lbzip2 -f -d -n %d %s" % (nth,tbzfile)
    out,err,status = sysrun(cmd)
    if not status:
        raise RuntimeError(out + err)
    # Unzip tar
    cmd = "tar -xf " + tarfile + " --directory=" + untbzdir #dirname(tarfile)
    out,err,status = sysrun(cmd) 
    if not status:
        raise RuntimeError(out + err)
    # Remove tar
    cmd = "rm -f " +  tarfile
    out,err,status = sysrun(cmd)  
    assert(status)  
    
def dm4tomrc(fsrc,fdst):   
    cmd     = "dm2mrc %s %s " % (fsrc, fdst)  
    out,err,status = sysrun(cmd) 
    #print out+err
    assert(status)
    
def stackmrcs(fsrc,fdst):
    cmd     = "newstack %s %s " % (fsrc, fdst)  
    out,err,status = sysrun(cmd) 
    #print out+err
    assert(status)
    
def transpose_mrc(srcmrc,dstmrc):
    g,psize = mrc.load_psize(srcmrc)
    g       = np.ascontiguousarray(g.swapaxes(-1,-2))
    mrc.save(g,dstmrc,pixel_size=psize)
        

