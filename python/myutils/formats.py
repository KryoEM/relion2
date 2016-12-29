#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 17:26:46 2016

@author: worker
"""

import myutils.filenames as fn
from   myutils.utils import sysrun #,tprint #Base,,
from   os.path import join,dirname
import multiprocessing

def untbz(tbzfile,**kwargs):
    nth = kwargs.pop('nthreads',multiprocessing.cpu_count())
    ''' Uncompresses list of files from a tbz archive '''
    tarfile = fn.replace_ext(tbzfile, '.tar')
    # Unzip tbz
    cmd     = "lbzip2 -f -d -n %d %s" % (nth,tbzfile)
    out,err,status = sysrun(cmd)
    if not status:
        raise RuntimeError(out + err)
    # Unzip tar
    cmd = "tar -xf " + tarfile + " --directory=" + dirname(tarfile)
    out,err,status = sysrun(cmd) 
    if not status:
        raise RuntimeError(out + err)
    # Remove tar
    cmd = "rm -f " +  tarfile
    out,err,status = sysrun(cmd)  
    assert(status)  
    
def dm4tomrc(fsrc,fdst):   
    cmd     = "dm2mrc %s %s" % (fsrc, fdst)  
    out,err,status = sysrun(cmd) 
    assert(status)  
    
def tbz2mrc(tbzname,dstext,**kwargs):
    ''' Unzips and converts to mrc. Cleans the tbz and the dm4 files '''
    mname  = fn.file_only(tbzname) 
    sdir   = dirname(tbzname) 
    sname  = join(sdir,mname)    
    untbz(tbzname,**kwargs)    
    dm4s = sname + '*.dm4'
    dm4tomrc(dm4s,sname + dstext)
    out,err,status = sysrun('rm ' + dm4s)
    assert(status)      
