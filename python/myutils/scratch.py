# -*- coding: utf-8 -*-
"""
Created on Wed Oct 29 10:30:09 2014

Manages scratch directory with automated cleanup

Usage:
    
scratch.init('/scratch')
sratchpath = scratch.join(path)

@author: Oleg Kuybeda
"""
import psutil
import myutils.filenames as fn
from   myutils.utils import tprint,sysrun,Base
import os
import socket
from glob import glob
#%%
sch = None

def join(path):
    if sch is not None:
       return sch.join(path)    
    else:
        raise ValueError('Scratch has not been initialized yet !!!')

def init(base_path):
    global sch
#    # redirect output to string 
#    sys.stdout = mystdout = StringIO()    
    if os.path.exists(base_path):        
        sch = Scratch(base_path) 
        #return sys.stdout.getvalue(), '', True #'Created process scratch path %s \n' % sch.proc_path
    else:
        raise IOError('Path not found ! %s' % base_path)
    
def clean():
    if sch is not None:
        sch.clean()
    #return sys.stdout.getvalue(),'', True # 'Cleaned scartch %s \n' % sch.proc_path
        
def get_mypid_dirname(base_dir, prefix):
    ''' Construct directory name based on my pid '''
    return os.path.join(base_dir, prefix + str(os.getpid()))
    
def clean_orphaned_procdirs(base_dir, prefix):
    ''' Removes all directories with suffixes containing nonexistent process ids '''
    # get current list of processes
    pids = psutil.pids()    
    # scan dir names for scratch patterned directories
    proc_dirs = glob(os.path.join(base_dir,prefix+'*'))    
    # obtain scratch ids 
    sids = [int(pdir.rsplit('_')[-1]) for pdir in proc_dirs] #os.path.basename(pdir).replace(prefix, '')
    # convert to number
    #sids = [int(sid) for sid in sids]
    # remove directories that correspond to nonexistent process
    for k in range(len(sids)):
        if all(sids[k] != pid for pid in pids):
            tprint('Cleaning orphaned dir: %s' % proc_dirs[k])
            #shutil.rmtree(proc_dirs[k])
            cmd = 'rm -rf ' + proc_dirs[k]
            sysrun(cmd)    

class Scratch(Base):
    def __init__(self, base_path, *args, **kwargs):
        super(Scratch, self).__init__(*args, **kwargs)
        self.prefix     = 'worker_'
        host = socket.gethostname()
        host = host.replace('-','')
        self.base_path  = os.path.join(base_path, host)
        tprint('Initializing scratch in %s ' % self.base_path)        
        # create scratch subdir that mtaches my pid
        self.proc_path  = get_mypid_dirname(self.base_path,self.prefix)
        # create scratch directory for my own procid
        # tprint('Process scratch path %s' % self.proc_path)
        fn.mkdir_assure(self.proc_path)    
        # clean orphaned scratch directories
        clean_orphaned_procdirs(self.base_path, self.prefix)

    def clean(self):
        #if scratch is not None:
        tprint('Cleaning scratch %s' % self.proc_path)
        #tprint('Removing %s' % scratch)
        fn.rmtree_assure(self.proc_path)    
        clean_orphaned_procdirs(self.base_path, self.prefix)    

    def join(self,path):
        return os.path.join(self.proc_path,path)        


############### GARBAGE ########################################
