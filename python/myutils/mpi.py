#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 16:18:23 2016

Here are implemented main functions for running generic MPI jobs

@author: Oleg Kuybeda
"""

from   mpi4py import MPI
from   myutils.utils import tprint,part_idxs 
import time

def count_completed_elements(comm):
    size   = comm.Get_size()                
    rreqs  = [comm.irecv(source=i, tag=22) for i in range(size)]
    stats  = [r.Get_status() for r in rreqs]
    # clear received messages
    [r.wait() for r in rreqs if r.Get_status()]
    # count completed elements
    return sum(stats)
    
def scatter_list(init_fun,run_fun,finish_fun):
    ''' Evenly scatters a list of elements returned by calling init_fun among all MPI 
        ranks and processes sub-lists in parallel '''
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()            
    # master dispatches work to all slaves including master himself
    if rank == 0:
        # obtain elements for mpi processing
        elements = init_fun()
        #elements = [elements[0],elements[1]]
        nels     = len(elements)        
        elgroups = part_idxs(elements,nbatches=size)
        glens    = [len(g) for g in elgroups]
        tprint('MPI - %d ranks, tot elements %d, [min %d,max %d] elements per rank' % (size,nels,min(glens),max(glens)))
        # send all batches to all workers
        sreqs    = [comm.isend(elgroups[i], dest=i, tag=11) for i in range(size)]    
    # obtain my batch
    elgroup = comm.recv(source=0, tag=11)
    # call run_fun on each element 
    nreqs = [] # notification requests
    count = 0  # competion counter
    for el in elgroup:
        run_fun(el)
        # notify master about finishing element
        nreqs.append(comm.isend('element finished',dest=0,tag=22))
        # track progress in masters while doing my job as well
        if rank == 0:
            # count completed elements
            count += count_completed_elements(comm)
            tprint("==========>>> Completed %d out of %d elements <<<==========" % (count,nels))            
    #notify i am finished
    freq = comm.isend('finished',dest=0,tag=33)
    if rank == 0:
        # wait for all messages to finish transmition
        [sreq.wait() for sreq in sreqs]
        # track progress of remained elements
        while count < nels:            
            # count completed elements
            count += count_completed_elements(comm)
            tprint("==========>>> Completed %d out of %d elements <<<==========" % (count,nels))            
            time.sleep(.1)                        
        # wait until everybody finishes
        [comm.recv(source=i, tag=33) for i in range(size)]
        finish_fun(elgroup)   
    freq.wait() 
    


