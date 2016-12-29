#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 16:58:01 2016

@author: Oleg Kuybeda
"""

#%%
from   myutils import scratch
from   myutils import filenames as fn
from   myutils.formats import tbz2mrc
from   myutils import mrc
from   os.path import join
from   myutils.utils import sysrun,tprint,part_idxs 
import shutil
from   myutils import star 
from   mpi4py import MPI

MOVSUFF         = '_movie.mrc'
AVGSUFF         = '_avg.mrc'
ALNSUFF         = '_aligned.mrc'
SCRATCH_DIR     = 'Unblur'
MOVIE_DIR       = 'Movies'
  
####### FUNCTIONS ####################################
def tbz2mrc_name(ftbz):
    mname   = fn.file_only(ftbz)    
    sdir    = scratch.join(SCRATCH_DIR)    
    mrcin   = join(sdir,mname+MOVSUFF)
    return mrcin    
    
def extract_tbz(ftbz,nth):
    mname  = fn.file_only(ftbz) 
    # create scratch dir
    sdir   = scratch.join(SCRATCH_DIR)
    fn.mkdir_assure(sdir)    
    # ----- extract tbz file --------
    dsttbz = join(sdir,mname) +'.tbz'    
    # copy and extract tbz
    shutil.copyfile(ftbz,dsttbz)
    # convert tbz to mrc
    tbz2mrc(dsttbz,MOVSUFF,nthreads=nth)       

def write_unblur_script(dstmdir,mrcin,nth,unblurexe,nframes,angpix,do_movies,
                        dose_per_frame,vol,pre_exp):
    mname   = fn.file_only(mrcin)    
    cshfile = join(dstmdir,mname+'_unblur.com')
    logfile = join(dstmdir,mname+'_unblur.log')
    mrcavg  = join(dstmdir,mname+AVGSUFF)
    mrcout  = join(dstmdir,mname+ALNSUFF)
    mrcsft  = join(dstmdir,mname+'_shifts.txt')
    f = open(cshfile,'w')
    f.write('#!/usr/bin/env csh\n')
    f.write('setenv  OMP_NUM_THREADS %d\n' % nth)
    f.write('%s > %s << EOF\n' % (unblurexe,logfile))
    f.write('%s\n%d\n' % (mrcin,nframes))
    f.write('%s\n' % (mrcavg))
    f.write('%s\n%f\n' % (mrcsft,angpix))
    if dodose:
        f.write('YES %d\n%d\n%d\n' % (dose_per_frame,vol,pre_exp))
    else:
        f.write('NO\n')
        
    if do_movies:
        f.write('YES\n%s\n' % mrcout)
    else:
        f.write('NO\n')    
    # don't set expert options    
    f.write('NO\n')    
    f.write('EOF\n')        
    f.close()
    return cshfile
    
def write_summovie_script(dstmdir,mrcin,nth,sumexe,nframes,angpix,
                        first_frame,last_frame):
    mname   = fn.file_only(mrcin)    
    cshfile = join(dstmdir,mname+'_summovie.com')
    logfile = join(dstmdir,mname+'_summovie.log')
    mrcavg  = join(dstmdir,mname+AVGSUFF)
    mrcsft  = join(dstmdir,mname+'_shifts.txt')
    mrcfrc  = join(dstmdir,mname+'_frc.txt')
    f = open(cshfile,'w')
    f.write('#!/usr/bin/env csh\n')
    f.write('setenv  OMP_NUM_THREADS %d\n' % nth)
    f.write('%s > %s << EOF\n' % (sumexe,logfile))
    f.write('%s\n%d\n' % (mrcin,nframes))
    f.write('%s\n' % (mrcavg))
    f.write('%s\n' % mrcsft)
    f.write('%s\n%d\n%d\n%f\n' % (mrcfrc,first_frame,last_frame,angpix))
    f.write('NO\n')    
    f.write('EOF\n')        
    f.close()
    return cshfile
    
def unblurmicro(nth,ftbz,dstmdir,do_aligned_movies,
                dodose,dose_per_frame,vol,pre_exp,
                dosummovie,first_frame,last_frame):
    # Unblur directory in scratch
    mrcname = tbz2mrc_name(ftbz) 
    # obtain number of frames in the movie
    nframes = mrc.shape(mrcname)[0]
    angpix  = mrc.psize(mrcname)    
    # generate unblur csh script
    tprint("Running Unblur on %s" % mrcname)                
    unblur_csh = write_unblur_script(dstmdir,mrcname,nth,
                                     unblurexe,nframes,angpix,do_aligned_movies,
                                     dose_per_frame,vol,pre_exp)    
    # call unblur script
    out,err,status = sysrun('csh %s' % unblur_csh)  
    assert(status)      
    if dosummovie:
        # generate summovoe script
        tprint("Running Summovie on %s" % mrcname)                        
        sum_csh = write_summovie_script(dstmdir,mrcname,nth,sumexe,nframes,angpix,
                                        first_frame,last_frame)
        # call summovie script
        out,err,status = sysrun('csh %s' % sum_csh)  
        assert(status)  
        
    # clean scratch
    scratch.clean()   
    
def main_mpi(do_aligned_movies,dodose,dosummovie):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    # init scratch for each slave
    scratch.init('/scratch')
    key  = '_rlnMicrographMovieName'
            
    # master dispatches work to all slaves including master himself
    if rank == 0:
        # directory for micrographs
        dstmdir   = join(dstdir,MOVIE_DIR)
        fn.mkdir_assure(dstmdir)   
        # read all tbz files from star file
        tbzs      = star.getlist(starfile,key)
        if len(tbzs) == 0:
            raise(IOError('No micrographs found in %s!' % starfile))
        tbzgroups = part_idxs(tbzs,nbatches=size)
        tprint('Runnning MPI - %d ranks with ~%d micrographs per rank' % (size,len(tbzgroups[0])))
        if dosummovie and dodose:
            tprint('Selected subset of frames, disabling dose weighting !!!')
            dodose = False
        # send all batches to all workers
        sreqs     = [comm.isend(tbzgroups[i], dest=i, tag=11) for i in range(len(tbzgroups))]
    
    # obtain my batch
    tbzgroup = comm.recv(source=0, tag=11)
    #tbzgroup = [tbzgroup[0],tbzgroup[1]]
    # wait for all messages to finish transmition
    [sreq.wait() for sreq in sreqs]
    # process all micros in the batch        
    for tbz in tbzgroup:
        tprint('-'*80)
        tprint("Extracting %s ..." % (tbz))            
        # extract tbz file to movie in scratch
        extract_tbz(tbz,nth)            
        unblurmicro(nth,tbz,dstmdir,do_aligned_movies,
                    dodose,dose_per_frame,vol,pre_exp,
                    dosummovie,first_frame,last_frame)                
    if rank == 0:
        # wait until everybody finishes
        comm.isend('finished',dest=0,tag=22)
        [comm.recv(source=i) for i in range(size)]
        # construct star files with resulting micrograph lists     
        cmd = 'relion_star_loopheader rlnMicrographMovieName > %saverage_micrographs.star \n \
              ls %s/*%s >> %saverage_micrographs.star' % (dstdir,dstmdir,AVGSUFF,dstdir)
        out,err,status = sysrun(cmd)  
        assert(status)  
        if do_aligned_movies:
            cmd = 'relion_star_loopheader rlnMicrographMovieName > %saligned_movies.star \n \
                    ls %s/*%s >> %saligned_movies.star' % (dstdir,dstmdir,ALNSUFF,dstdir)
            out,err,status = sysrun(cmd)  
            assert(status)      
    
def get_parser():
    import argparse    
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@',
                                     description='Running unblur via MPI.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     epilog="Example: mpirun -n 1 `which unblurtbz.py` -i Import/job001/movies.star -o MotionCorr/job001/" 
                                     " -j 4 --save_movies -e /jasper/relion/Unblur/unblur_1.0.2/bin/unblur_openmp_7_17_15.exe \n"
                                     "Note: if first_frame_sum and last_frame_sum specified, then dose weighting will be disabled."
                                     "Update this python script to include dose weighting via summovie utility if needed.")
    parser.add_argument('-i','--input_star_file', help='Star file with tbz-compressed filenames.', 
                        default=argparse.SUPPRESS, type=str, required=True)
    parser.add_argument('-o','--output_dir', help='Output directory', 
                        default=argparse.SUPPRESS, type=str, required=True)  
    parser.add_argument('-j','--threads', help='Number of threads',default = 4, type=int, required=False)                    
    parser.add_argument('-s','--save_movies', help='Flag to save aligned movies', 
                        default=True, type=bool, required=False)   
    parser.add_argument('-d','--do_dose', help='Flag to do  dose weighting', 
                        default=False, type=bool, required=False)   
    parser.add_argument('-a','--save_aligned_movies', help='Flag whether to save aligned movies', 
                        default=False, type=bool, required=False)   
    parser.add_argument('-df','--dose_per_frame', help='', 
                        default=0.0, type=float, required=False)  
    parser.add_argument('-v','--voltage', help='Voltage used for dose weighting', 
                        default=0.0, type=float, required=False)      
    parser.add_argument('-p','--pre_exp', help='Pre exposure used for dose weighting', 
                        default=0.0, type=float, required=False)      
    parser.add_argument('-f','--first_frame_sum', help='First frame to average (starting from 0)', 
                        default=0, type=int, required=False)   
    parser.add_argument('-l','--last_frame_sum', help='Number of last frame to average (starting from 0)', 
                        default=0, type=int, required=False)   
    parser.add_argument('-e','--unblur_exe', help='Path to unblur executable.', 
                        default=argparse.SUPPRESS, type=str, required=True)
    return parser      
    
#%%##############################################


###### Main starts here #######################################    
if __name__ == "__main__":   
    kwargs = vars(get_parser().parse_known_args()[0])
    # call main function
    main_mpi(**kwargs)      


#%%
starfile  = '/jasper/temp/betagal1/Import/job001/movies.star'
dstdir    = '/jasper/temp/betagal/MotionCorr/job004/'
unblurexe = '/jasper/relion/Unblur/unblur_1.0.2/bin/unblur_openmp_7_17_15.exe'
sumexe    = '/jasper/relion/Summovie/summovie_1.0.2/bin/sum_movie_openmp_7_17_15.exe'

nth       = 4
dodose    = False
dose_per_frame = 1.0
vol       = 300
pre_exp   = 1.0
do_aligned_movies = True
dosummovie = True
first_frame = 3
last_frame = 20


           


       
       
    




