# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 21:47:31 2014

@author: worker
"""
from __future__ import print_function
import sys
import time
import pickle
import subprocess as sp
import numpy as np
from   time import strftime, localtime 
import signal 
import socket
from   cStringIO import StringIO
#import os
#import psutil

#from wmi import WMI

now=time.time

def rand_rot_mat(sz):
    M   = np.random.rand(sz[0],sz[1])
    q,r = np.linalg.qr(M)   
    return q

def sizeof(t):
    return np.dtype(t).itemsize

def suggest_radix2357(x):
    e2 = np.ceil(np.log2(x))
    # all possible powers of 2 that work with one multiplier of 3,5,7
    es  = np.arange(e2-4,e2+1)
    r2  = np.int32(2**es)
    # possible sizes
    szs = np.kron(r2[None,:],np.int32([1,3,5,7])[:,None]).flatten()
    return szs
    
def prev_radix2357(x):
    szs     = suggest_radix2357(x)
    d       = x - szs
    d[d<0]  = x
    return szs[np.argmin(d)]
    
def next_radix2357(x):
    szs     = suggest_radix2357(x)
    d       = szs - x
    d[d<0]  = szs.max()
    return szs[np.argmin(d)]

              
def next_radix2357_coupled(x,y):
    # make k radix 2357 and y even
    szs     = suggest_radix2357(x)
    d       = szs - x
    sugy    = np.round(y*szs/x)
    # remove undesired outcomes
    d[np.logical_or(d<0,np.mod(sugy,2)==1)]  = szs.max()
    idx     = np.argmin(d)
    return int(szs[idx]),int(sugy[idx])               
               
def nextmult(val,mult):
    rem = np.mod(val,mult)
    return val + np.mod(mult-rem,mult)

def prevmult(val,mult):
    return val - np.mod(val,mult) 

def combine_trans(t1,t2):
    x1,y1,ang1 = t1[:,0],t1[:,1],t1[:,2]
    x2,y2,ang2 = t2[:,0],t2[:,1],t2[:,2]    
    xa2 = np.cos(ang1)*x2 - np.sin(ang1)*y2
    ya2 = np.sin(ang1)*x2 + np.cos(ang1)*y2    
    return np.float32(zip(x1+xa2,y1+ya2,ang1+ang2)) 

def rand_subset(tot,n):
    pidxs = np.arange(tot,dtype='int32')
    np.random.shuffle(pidxs)
    return pidxs[:n].copy()    

def match_arrays(A,B):
    ''' return indices of B that match elements in A '''
    B_unique_sorted, B_idx = np.unique(B, return_index=True)
    B_in_A_bool = np.in1d(B_unique_sorted, A, assume_unique=True)
    return B_idx[B_in_A_bool].tolist()

class Timer(object):
    def __init__(self, name=None):
        self.name = name

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:
            print('[%s]' % self.name)
        print('Elapsed: %s' % (time.time() - self.tstart))

class Base(object):
    def __init__(self, *args, **kwargs):
        pass

def myceil(base,multiple):
    return np.ceil(base/multiple)*multiple
    
def set_signal_handler(sighandler):
    signal.signal(signal.SIGHUP,sighandler)
    signal.signal(signal.SIGINT,sighandler)
    signal.signal(signal.SIGTERM,sighandler)
    signal.signal(signal.SIGQUIT,sighandler)
#    for i in [x for x in dir(signal) if x.startswith("SIG")]:
#      try:
#        signum = getattr(signal,i)
#        signal.signal(signum,sighandler)
#      except RuntimeError:
#        tprint("Skipping %s"%i)   
    
def tprint(*args, **kwargs):
    ''' Custom print function the adds time '''
    time = strftime("%m/%d %H:%M|", localtime())
    #time = strftime("%m/%d %H:%M:%S|", localtime())
    host = socket.gethostname() + "|"
    #__builtins__.print(time)
    res = print(host+time,*args,**kwargs)
    sys.stdout.flush()
    return res    

def pickle_size(obj):
    return sys.getsizeof(pickle.dumps(obj))+1  

def std2string():
    sys.stdout = StringIO()    
    #sys.stderr = StringIO()    
    #tprint('Redirected stdout to string ...')
    #return sys.stdout.getvalue(), '', True      
    
def sysrun(cmd, **kwargs):
    ce   = kwargs.pop('err_check', True)
    verb = kwargs.pop('verbose', False)
    if verb:
        tprint(cmd)
    process = sp.Popen(cmd,shell=True,close_fds=True,
                       stdout=sp.PIPE,stderr=sp.PIPE)
    # wait for the process to terminate
    out, err = process.communicate()
    errcode  = process.returncode
    if (errcode != 0) and ce:
        print(err)
        status = False
    else:
        status = True        
        #raise RuntimeError('System call failed!')
    return out,err,status        
        
def ispow2(number):
    return (number & (number-1) == 0) and (number != 0)
  
def batch_idxs(n, batch, iter):
    return range(int(batch*iter), int(min(batch*(iter+1), n))) 
    
def nextpow2(i):
    """
    Find the next power of two
  
    >>> nextpow2(5)
    8
    >>> nextpow2(250)
    256
    """
    # do not use numpy here, math is much faster for single values
    buf = np.ceil(np.log(i) / np.log(2))
    return int(np.power(2, buf)) 
    
def prevpow2(i):
    """ Find previous power of two """
    # do not use numpy here, math is much faster for single values
    buf = np.floor(np.log(i) / np.log(2))
    return int(np.power(2, buf))    
    
def str2float(x):
    ''' Will convert to float if possible, else leave string '''
    num = x.replace('.','')
    return float(x) if unicode(num).isnumeric() else x    
    
def params2dict(fname):
    with open(fname, "r") as f:
        lines = f.readlines()
    d = dict()      
    for line in lines:  
        line = line.replace('\n','')
        line = line.replace('--','')
        splt = line.split('=')  
        d.update({splt[0]:str2float(splt[1])})    
    return d        
    
def find_nearest_idx(array, value):
    idx = (np.abs(np.array(array)-value)).argmin()
    return idx
    
def find_brackets( aString ):
   ''' This will find return the string enclosed in the outermost bracket. 
        If None is returned, there was no match.'''
   if '{' in aString:
      match = aString.split('{',1)[1]
      open = 1
      for index in xrange(len(match)):
         if match[index] in '{}':
            open = (open + 1) if match[index] == '{' else (open - 1)
         if not open:
            return match[:index]    
          
def array2str(array, frmt='%.2f'):        
    return ','.join(map(lambda x:frmt%x, list(array))) 
    
def part_idxs(els, **kwargs):
    ''' Create partition of elements that satisfy either nbatches, or batch size '''
    nchunks = kwargs.pop('nbatches',None)
    N       = np.float32(len(els))
    if nchunks is None:
        chunksize = int(kwargs.pop('batch',1))
        nchunks   = int(np.ceil(N/chunksize))
    else:
        nchunks = min(nchunks,len(els))
    chunksize = int(np.floor(N/nchunks))
    chunksize = max(chunksize,1)
    extra   = N-nchunks*chunksize  
    pos     = 0
    l       = list()
    for k in range(int(extra)):
        l.append(els[pos:pos+chunksize+1])
        pos += chunksize+1        
    for k in range(int(extra),int(nchunks)):
        l.append(els[pos:pos+chunksize])
        pos += chunksize
    return l

def save_params(dct, out_fname):
    with open(out_fname, "w") as outfile:
        for key, value in dct.items(): # returns the dictionary as a list of value pairs -- a tuple.
            if not value=='skip':
                outfile.write('--%s=%s\n' % (key, str(value)))
                
def nang2angs(nang):
    return np.float32(np.linspace(0.0,(2*np.pi)*(nang-1)/nang,nang))               

################## GARBAGE #########################################
#def memory():
#    w = WMI('.')
#    result = w.query("SELECT WorkingSet FROM Win32_PerfRawData_PerfProc_Process WHERE IDProcess=%d" % os.getpid())
#    return int(result[0].WorkingSet)

#def memory_usage():
#    # return the memory usage in MB
#    process = psutil.Process(os.getpid())
#    return process.memory_full_info()[7]

#def dict2params(fname,d):
#    lines = list()
#    with open(fname, "w") as f:
#        for p in d:
#            lines.append("--%s=%f" % p)
#    
#    print lines
        
#        lines = f.readlines()
#    d = dict()      
#    for line in lines:  
#        line = line.replace('\n','')
#        line = line.replace('--','')
#        splt = line.split('=')  
#        d.update({splt[0]:str2float(splt[1])})    
#    return d  
        
#class ProgressBar(Base):
#    def __init__(self,tot_count,*args, **kwargs):
#        super(ProgressBar, self).__init__(*args,**kwargs)
#        self.tot_count = tot_count + 1
#        ttle = kwargs.get('title')
#        if ttle:
#            sys.stdout.write(ttle+"\n")
#        # setup toolbar
#        sys.stdout.write("[%s]" % (" " * tot_count))
#        sys.stdout.flush()
#        sys.stdout.write("\b" * self.tot_count) # return to start of line, after '['
#    def __del__(self):
#        pass        
#    def update(self):
#        sys.stdout.write("*")
#        sys.stdout.flush()
#        #self.tot_count -= 1
#    def finalize(self):
#        sys.stdout.write("\n")                              
                      
#def apply_argsort(a, axis=-1):
#     i = list(np.ogrid[[slice(x) for x in a.shape]])
#     i[axis] = a.argsort(axis)
#     return a[i]                

#import argparse
#class SmartFormatter(argparse.ArgumentDefaultsHelpFormatter):
#    def format_epilog(self, formatter):
#        return self.epilog
#    def _split_lines(self, text, width):
#        # this is the RawTextHelpFormatter._split_lines
#        if text.startswith('R|'):
#            return text[2:].splitlines()  
#        return argparse.ArgumentDefaultsHelpFormatter._split_lines(self, text, width)
#           
    
#class IntRange(object):
#    ''' For testing argument params '''
#    def __init__(self, start, stop=None):
#        if stop is None:
#            start, stop = 0, start
#        self.start, self.stop = start, stop
#
#    def __call__(self, value):
#        value = int(value)
#        if value < self.start or value >= self.stop:
#            raise argparse.ArgumentTypeError('value outside of range')
#        return value    
        
        
#def nextpow2_list(X):
#    return (nextpow2(x) for x in X)
#        
#def reload_all(globs):
#    modulenames = set(sys.modules)&set(globs)
#    #allmodules  = [sys.modules[name] for name in modulenames]
#    modulenames = list(modulenames-set(['sys', '__builtin__']))
#    for mod in modulenames:
#        print "Reloading " + mod
#        reload(sys.modules[mod])
        
#def nbytes(*args):
#    sizes = (arg.nbytes for arg in args)
#    return sum(sizes)
                

                  