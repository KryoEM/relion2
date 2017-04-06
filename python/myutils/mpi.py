#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 16:18:23 2016

Here are implemented main functions for running generic MPI jobs

@author: Oleg Kuybeda
"""
#%%
from   mpi4py import MPI
from   myutils.utils import tprint,enum  #part_idxs
import time

TAGS   = enum('READY', 'DONE', 'CLOSED', 'EXIT', 'START','WAITING')
#%%

def __worker_iter(run_fun,comm,status): 
    # notify master I'm ready  
    comm.send(None, dest=0, tag=TAGS.READY)                    
    task = comm.recv(source=0, tag=MPI.ANY_TAG,status=status)        
    tag  = status.Get_tag()    
    if tag == TAGS.START:
        # Do the work here
        run_fun(task)
        return TAGS.DONE
    if tag == TAGS.EXIT:
        comm.send(None, dest=0, tag=TAGS.CLOSED)                                            
        return TAGS.EXIT  
    return TAGS.WAITING
            
def __master_iter(comm,status,tasks,task_index,closed_index):
    ntasks = len(tasks)  
    comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,status=status)        
    source = status.Get_source()
    tag    = status.Get_tag()
    if tag == TAGS.READY:    
        # Worker is ready, so send it a task
        if task_index < ntasks:
            tprint("==========>>> Sending task %d/%d to worker %d <<<==========" % (task_index+1,ntasks,source))
            comm.send(tasks[task_index], dest=source, tag=TAGS.START)
            task_index += 1
        else:
            #tprint("==========>>> Sending EXIT to worker %d <<<==========" % (source))
            comm.send(None, dest=source, tag=TAGS.EXIT)  
    if tag == TAGS.CLOSED:
        #tprint("========>>> Worker %d completed %d out of %d tasks <<<========" % (source,done_index+1,ntasks))  
        closed_index += 1                          
    return task_index,closed_index
                    
def scatter_list(get_all_tasks,run_fun,finish_fun):
    ''' Evenly scatters a list of elements returned by calling init_fun among all MPI 
        ranks and processes sub-lists in parallel '''
    comm   = MPI.COMM_WORLD
    rank   = comm.Get_rank()
    size   = comm.Get_size() 
    name   = MPI.Get_processor_name()
    status = MPI.Status()
    if size < 2:
        raise(IOError("Please use 2 ranks at least for MPI processing!"))
    # master does only slave management work - no computations
    if rank == 0:
        # obtain elements for mpi processing
        tasks = get_all_tasks()
        # scheduled task index and closed workers index
        task_index,closed_index = 0,0
        tprint('MPI - %d ranks, tot tasks %d' % (size,len(tasks)))
        while closed_index < size-1:
            time.sleep(0.1)
            task_index,closed_index = __master_iter(comm,status,tasks,task_index,closed_index)   
        # finilize run
        finish_fun(tasks)            
        tprint("MASTER %s,rank %d finished processing %d tasks" % (name,rank,len(tasks)))
    else:
        # workers only do the processing work
        while True:
            # do only workers stuff
            if __worker_iter(run_fun,comm,status) == TAGS.EXIT: 
                break    
            time.sleep(0.1)
        #tprint("WORKER %s,rank %d finished" % (name,rank))
                                         
def verify(out,err,status):
    if not status:
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        print "######## ERROR on RANK %d ###########\n" % rank
        print out
        print err
        MPI.COMM_WORLD.Abort(1)

####### GARBAGE #############
    #pending  = comm.iprobe(source=0, tag=MPI.ANY_TAG,status=status)
    #if pending:

    #elif tag == tags.DONE:
        #results = data
        #print("Got data from worker %d" % source)
    #elif tag == tags.EXIT:
    #    print "Worker %d exited." % source
    #    closed_workers += 1

#    status = MPI.Status()
#    source = status.Get_source()
#    tag    = status.Get_tag()
#    if status.count>0:
#        req.wait()

#def count_completed_elements(comm):
#    size   = comm.Get_size()                
#    rreqs  = [comm.irecv(source=i, tag=22) for i in range(size)]
#    stats  = [r.Get_status() for r in rreqs]
#    # clear received messages
#    lcount = sum([len(rreqs[i].wait()) for i in range(size) if stats[i]==True])
#    # count completed elements
#    return sum(stats)
    #status = MPI.Status()
    #if status.count>0:
    #    req.wait()
    #tag    = status.Get_tag()

        #elgroups = part_idxs(elements,nbatches=size)
        #glens    = [len(g) for g in elgroups]
        # send all batches to all workers
        #sreqs    = [comm.isend(elgroups[i], dest=i, tag=11) for i in range(size)]    
    # obtain my batch
    #elgroup = comm.recv(source=0, tag=11)
    # call run_fun on each element 
#    nreqs = [] # notification requests
#    count = 0  # competion counter
#    for el in elgroup:
#        #run_fun(el)
#        # notify master about finishing element
#        nreqs.append(comm.isend('1',dest=0,tag=22))
#        # track progress in masters while doing my job as well
#        if rank == 0:
#            # count completed elements
#            count += count_completed_elements(comm)
#            tprint("==========>>> Completed %d out of %d elements <<<==========" % (count,nels))            
#    #notify i am finished
#    freq = comm.isend('finished',dest=0,tag=33)
#    if rank == 0:
#        # wait for all messages to finish transmition
#        [sreq.wait() for sreq in sreqs]
#        # track progress of remained elements
#        while count < nels:            
#            # count completed elements
#            count += count_completed_elements(comm)
#            tprint("==========>>> Completed %d out of %d elements <<<==========" % (count,nels))            
#            time.sleep(.1)                        
#        # wait until everybody finishes
#        [comm.recv(source=i, tag=33) for i in range(size)]
#        finish_fun(elgroup)   
#    freq.wait() 
