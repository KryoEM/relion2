# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 21:55:09 2014

@author: worker
"""

import posix_ipc
import pickle
import mmap
from   myutils.utils import Base,pickle_size,now,tprint
import psutil
import os
import time
import fcntl
import warnings
import itertools
    
#%% Shared Memory functionality
class Lock(Base):    
    def __init__(self, *args, **kwargs):
        super(Lock, self).__init__(*args, **kwargs)
        self.__fname = kwargs.get('filename')
        # This will create it if it does not exist already
        self.handle = open(self.__fname, 'w')
    
    # Bitwise OR fcntl.LOCK_NB if you need a non-blocking lock 
    def acquire(self):
        #print "Before acquiring %s" % self.__fname
        fcntl.flock(self.handle, fcntl.LOCK_EX)
        #print "After acquiring %s" % self.__fname
        
    def release(self):
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        
    def __del__(self):
        self.handle.close() 
        
class SharedMemory(Base):
    ''' Pickles and unpickles data stroed in shared memory '''
    def __init__(self, *args, **kwargs):
        super(SharedMemory, self).__init__(*args, **kwargs)
        # Open or Create the shared memory and the semaphore.
        self.mem_name  = kwargs.get('mem_name')
    def __del__(self):
        pass            
    def load(self):
        # try to attach to an existing shared memory
        memory  = posix_ipc.SharedMemory(self.mem_name, posix_ipc.O_CREAT)
        if memory.size == 0 :
            l = []
        else:
            memmap  = mmap.mmap(memory.fd, memory.size)
            try:
                l = pickle.load(memmap)  
            except KeyError:
                # something happened reset the sharing object
                warnings.warn("Something wrong with shared resource control, resetting !", RuntimeWarning)
                l = []                
                
            memmap.close()  
            memory.close_fd()
        return l        
    def save(self, l):
        if not l:
            self.reset()
            # Don't save empty list
            return               
        # release previous memory is exists            
        try:             
            memory  = posix_ipc.SharedMemory(self.mem_name, posix_ipc.O_CREX)
        except posix_ipc.ExistentialError: 
            self.reset()            
        psize   = pickle_size(l)
        # save a new shared memory
        memory  = posix_ipc.SharedMemory(self.mem_name, posix_ipc.O_CREAT, size=psize)         
        memmap  = mmap.mmap(memory.fd, psize)  
        pickle.dump(l, memmap) 
        memmap.flush()
        memmap.close()  
        memory.close_fd() 
    def reset(self):
        posix_ipc.unlink_shared_memory(self.mem_name)
             
class ProtectedSharedMemory(SharedMemory):
    ''' Manages file-lock protected shared memory '''
    def __init__(self, *args, **kwargs):
        mem_name  = kwargs.get('mem_name')
        super(ProtectedSharedMemory, self).__init__(*args, **kwargs)   
        self.__lock = Lock(filename = "/tmp/" + mem_name + ".lock")
    def __del__(self):
        pass
    def load_lock(self):
        self.__lock.acquire()
        return super(ProtectedSharedMemory, self).load()                  
    def save_unlock(self, obj):
        super(ProtectedSharedMemory, self).save(obj)
        self.__lock.release()      
        
class TimedPriority(Base):
    ''' Class that controls priority changes as a function of pending time '''    
    MAX_TIME    = float(30*60) # defines maximum memory time
    RUNNING     = int(0)
    IDLE        = int(1)
    PENDING     = int(2)
    
    def __init__(self):
        super(TimedPriority, self).__init__()
        self.run_time    = 0.0
        self.idle_time   = 0.0
        self.pend_time   = 0.0
        self.state       = TimedPriority.IDLE
        self.access_time = now()

    @staticmethod    
    def __inc(value, diff):
        hist = max(1.0 - float(diff)/TimedPriority.MAX_TIME,0.0);
        return value*hist + diff        
    def __run_inc(self, tdiff):
        self.pend_time = 0.0
        self.run_time  = TimedPriority.__inc(self.run_time, tdiff)
    def __idle_inc(self, tdiff):
        self.pend_time = 0.0       
        self.idle_time = TimedPriority.__inc(self.idle_time, tdiff)
    def __pend_inc(self, tdiff):
        self.pend_time = TimedPriority.__inc(self.pend_time, tdiff)
        
    def __update_time(self):
        curtime = now()
        tdiff   = curtime - self.access_time        
        options = {TimedPriority.PENDING : self.__pend_inc,
                   TimedPriority.RUNNING : self.__run_inc,
                   TimedPriority.IDLE    : self.__idle_inc}
        options[self.state](tdiff)     
        self.access_time = curtime            
        
    def priority(self):
        self.__update_time()
        #return self.pend_time/max(self.run_time, 1.0) \
        #        if self.state == TimedPriority.PENDING else 0.0
        return self.pend_time if self.state == TimedPriority.PENDING else 0
    
    def set_pending(self):
        self.__update_time()
        self.state = TimedPriority.PENDING
        
    def set_running(self):
        self.__update_time()
        self.state = TimedPriority.RUNNING
        
    def is_running(self):
        return self.state == TimedPriority.RUNNING
    
    def set_idle(self):
        self.__update_time()
        self.state = TimedPriority.IDLE    

    def is_pending(self):
        self.__update_time()        
        return self.state == TimedPriority.PENDING     
        
    def release_pending(self):
        if self.state == TimedPriority.PENDING:
            self.set_idle()
        
class ResourceControlBlock(Base):
    NO_RESOURCE = -1       
    def __init__(self,pid,n_res,prio_level):
        # current assigned resource id
        #self.rid        = ResourceControlBlock.NO_RESOURCE    
        # current assigned process id
        self.pid        = pid    
        self.prio_level = prio_level
        # allocate array of priorities for each resource
        self.parray     = [TimedPriority() for r in range(n_res)]  
        # Create control blocks in an idle state
        self.set_idle()

    def priority(self,rid):
        ''' return priority of resource res_idx '''
        return self.prio_level*self.parray[rid].priority()
        
    def busy_rids(self):
        rids = [r for r in range(len(self.parray)) if self.parray[r].is_running()];
        return rids        
        
    def set_prio_level(self,prio_level):
        self.prio_level = prio_level
    
    def set_pending(self,rid):
        self.parray[rid].set_pending()

    def set_running(self,rid):
        # idle requests for all resources
        #self.set_idle()
        # set rid to run state
        #self.rid = rid
        self.parray[rid].set_running() 
                       
    def set_idle(self):
        #self.rid = ResourceControlBlock.NO_RESOURCE
        # idle all resource request priorities
        for p in self.parray:
            p.set_idle() 

    def release_pending(self):
        for p in self.parray:
            p.release_pending() 
        
    def has_resource(self): 
        runloc = [p.is_running() for p in self.parray]
        return any(runloc)        
        #return self.rid != ResourceControlBlock.NO_RESOURCE    
        
    def has_resources(self,rids): 
        runloc = [self.parray[r].is_running() for r in rids]
        return all(runloc)        
    
    def is_pending(self,rid):
        return self.parray[rid].is_pending()        

class SharedResources(Base):
    """Manages a lock-proteced shared access to resources """ 
    def __init__(self, **kwargs):      
        super(SharedResources, self).__init__()
        self.rids = None     
        self.mem  = ProtectedSharedMemory(**kwargs)    
    def __del__(self):
        pass
        #self.disconnect_myproc()
    def init(self, rids):
       self.rids = rids
    @staticmethod    
    def __clean_orphaned(control_list):
        ''' cleans control blocks of nonexistent processes '''
        # get all pids
        #control_list
        pids  = psutil.pids()       
        for cb in control_list[:]:  
            #print cb.pid
            if all(cb.pid != pid for pid in pids):
                tprint("Found orphaned shared resource process " + str(cb.pid) + " - removing its control block")               
                control_list.remove(cb)
        #print "Len of list %d" % len(control_list)                
        return control_list  
        
    def reset(self):
        self.mem.reset()   
        
    def print_running(self):
        # read control list from the shared memory
        control_list = SharedResources.__clean_orphaned(self.mem.load_lock()) 
        rids = [cb.rid for cb in control_list]            
        print rids        
        
    def print_priority(self,devid):
        # read control list from the shared memory
        control_list = SharedResources.__clean_orphaned(self.mem.load_lock()) 
        pend_prio = [cb.priority(devid) for cb in control_list]
        print pend_prio         
           
    def allocate(self,in_req_rids=[],**kwargs):        
        ''' Allocates an available GPU.
            req_rid - requested rid'''
        mypid = kwargs.pop('mypid',os.getpid())
        # priority level for allowing background GPU processing
        prio_level = kwargs.pop('prio_level',1.0)
                
        # read control list from the shared memory
        control_list = SharedResources.__clean_orphaned(self.mem.load_lock()) 
        
        # find my control block
        mcb = [cb for cb in control_list if cb.pid == mypid]
        if mcb: 
            # get an existing control block
            assert(len(mcb) == 1)
            mcb = mcb[0] 
            control_list.remove(mcb)  
            # update my process priority level
            mcb.set_prio_level(prio_level)
        else:   # allocate a new one
            # create array of resource priorities 
            mcb = ResourceControlBlock(mypid,len(self.rids),prio_level) 
            
        if len(in_req_rids)==0:
            if not mcb.has_resource():            
                # look for any device available
                req_rids = set(self.rids[:])
            else:
                # nothing to ask more
                req_rids = set([])
        else:
                #has_rids = mcb.has_resources(req_rids)
                # look only for the requested devices only
                req_rids = set(in_req_rids) - set(mcb.busy_rids())    
                                    
        # remove all busy gpus        
        busy_rids  = [cb.busy_rids() for cb in control_list]
        busy_rids  = set(itertools.chain.from_iterable(busy_rids))
        #print len(control_list),req_rids, busy_rids
        # resources that asked but unavailable
        busy_rids = list(req_rids.intersection(busy_rids))
        # resources that asked and available
        req_rids = list(req_rids.difference(busy_rids))  

        # set pending priorities on busy resources
        for brid in busy_rids:
            mcb.set_pending(brid)
        
        # check if I can get available resource
        for arid in req_rids:
            # get all pending priorities 
            pend_prio = [cb.priority(arid) for cb in control_list if cb.is_pending(arid)]
            # decide if my process is eligible for allocation and allocate
            myprio = mcb.priority(arid)
            #print "myprio % .3f" % myprio, [pp for pp in pend_prio], "\n"
            
            if all(myprio >= pp for pp in pend_prio):
                #tprint("Allocated resource " + str(avail_rids[arid]) + ", priority=" + str(myprio))
                mcb.set_running(arid)
                break
            else:
                # this resource avaiable, but there are higher prio pending processes, 
                # so go pending now
                mcb.set_pending(arid)
                                
        # write control blocks back to the memory
        mybusy = mcb.busy_rids()
        #if (in_req_rids != None and set(in_req_rids) == set(mybusy)) or \
        # No specific gpu was asked for, so release pending         
        if ((len(in_req_rids) == 0) and (len(mybusy) > 0)):
                mcb.release_pending()   

        control_list.append(mcb)
        self.mem.save_unlock(control_list)  
        return mybusy     
        
    def release(self,**kwargs):
        mypid = kwargs.pop('mypid',os.getpid())

        # read control list from the shared memory
        control_list = SharedResources.__clean_orphaned(self.mem.load_lock())        
        # get my control block            
        mcb   = [cb for cb in control_list if cb.pid == mypid]
        # my control block should be registered in the list !!
        if not mcb:
            #print "saving %d" % len(control_list)
            self.mem.save_unlock(control_list)        
            return
        #assert(mcb) 
        # update my process id
        assert(len(mcb) == 1)
        mcb = mcb[0]
        control_list.remove(mcb)   
             
        mcb.set_idle()
        # write control blocks back to the memory
        control_list.append(mcb)
        self.mem.save_unlock(control_list)   
        
    def disconnect_myproc(self,**kwargs):
        ''' disconnect my process from the control list '''
        mypid = kwargs.pop('mypid',os.getpid())        
        
        # read control list from the shared memory
        control_list = SharedResources.__clean_orphaned(self.mem.load_lock())        
        # get my control block
        tprint('Disconnecting my process %d' % mypid)
        mcb   = [cb for cb in control_list if cb.pid == mypid]
        # my control block should be registered in the list !!
        if not mcb:
            #print "saving %d" % len(control_list)
            self.mem.save_unlock(control_list)        
            return
        # update my process id
        mcb = mcb[0]
        control_list.remove(mcb)   
        self.mem.save_unlock(control_list)           
        
    def allocate_blocked(self,*args,**kwargs):
        ''' Tries allocating resource until succeess '''
        #rid = ResourceControlBlock.NO_RESOURCE
        if not args:
            while True:
                rid = self.allocate()
                if rid != []:
                    return rid
                else:
                    time.sleep(1.0)
        else:            
            while True:
                req_ids = args[0]
                rids = self.allocate(req_ids,**kwargs)
                #print rids
                if set(rids) == set(req_ids):
                    return rids                    
                else:
                    time.sleep(0.5)
        
#%% Unit tests    
if __name__ == "__main__":   
    tprint("Run this script using many processes one after another ...")
    sr = SharedResources([0], mem_name = "SHAREDRESCONTROL")
    in_str = ''
    while in_str != 'q':
        tprint("Allocating resource for this process ...") 
        tprint("Allocated resource " + str(sr.allocate_blocked()))
        in_str = raw_input("Press ENTER for continue, q for quit ... ")
        tprint("Releasing process resource")
        sr.release()        
    tprint("Quitting ...")   
    
    
#################### GARBAGE #################################################
#    def set_pending_all(self):
#        for p in self.parray:
#            p.set_pending() # create in a pending state

#class ResourcePriority(TimedPriority):
#    ''' Manages resource priorities '''
#    def __init__(self, procid):
#        super(ResourcePriority, self).__init__()
##        self.rid  = ResourcePriority.NO_RESOURCE    # current assigned gpu id
##        self.pid  = procid    # current assigned process id
#        self.set_pending()      # create in a pending state
##    def set_running(self, rid):
##        self.rid = rid
##        super(ResourcePriority, self).set_running()
#    def set_idle(self):
#        self.rid = ResourcePriority.NO_RESOURCE
#        super(ResourcePriority, self).set_idle()
#    def has_resource(self):
#        return self.rid != ResourcePriority.NO_RESOURCE    