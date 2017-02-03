 #! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 16:58:01 2016

@author: Oleg Kuybeda
"""

#%%
from   myutils import scratch
from   myutils import filenames as fn
from   myutils.formats import dm4tomrc,stackmrcs,untbz,transpose_mrc
from   myutils import mrc
from   os.path import join,dirname,splitext
from   myutils.utils import sysrun,tprint 
import shutil
from   myutils import star 
from   myutils import mpi
from   functools import partial
import argparse  
import glob 

MOVSUFF         = '_movie.mrc'
AVGSUFF         = '_avg.mrc'
ALNSUFF         = '_aligned.mrc'
SCRATCH_DIR     = 'Unblur'
MOVIE_DIR       = 'Movies'
# wildcard for gains filename
GAINS_KEY       = 'Gain Ref'
  
####### FUNCTIONS ####################################
def tbz2mrc_name(ftbz):
    mname   = fn.file_only(ftbz)    
    sdir    = scratch.join(SCRATCH_DIR)    
    mrcin   = join(sdir,mname+MOVSUFF)
    return mrcin    
    
def gain2mrc(basedir): 
    # destination dir
    sdir     = scratch.join(SCRATCH_DIR)    
    path     = join(basedir,'*%s*.dm4' % GAINS_KEY)
    gaindm4  = glob.glob(path)
    if len(gaindm4) == 0:
            raise IOError('Gain dm4 file not found in %s !!!' % basedir)
    gaindm4  = gaindm4[0]
    gainmrc  = join(sdir,'gain.mrc')
    dstdm4   = join(sdir,'gain.dm4')
    # link dm4 with a simpler filename    
    cmd = "ln -s \'%s\' %s" % (gaindm4,dstdm4)    
    out,err,status = sysrun(cmd)  
    assert(status)      
    dm4tomrc(dstdm4,gainmrc) 
    cmd = "rm " +  dstdm4
    out,err,status = sysrun(cmd)  
    assert(status)      
    return gainmrc
    
def multgains(srcmrc,gainmrc,dstmrc):
    cmd = 'clip mult -n 16 %s %s %s' % (srcmrc,gainmrc,dstmrc)
    out,err,status = sysrun(cmd) 
    assert(status)        
    
def tbz2mrc(srcdir,tbzname,dstext,**kwargs):
    ''' Unzips and converts to mrc. Cleans the tbz and the dm4 files '''
    # copy gains and convert to mrc
    mname    = fn.file_only(tbzname) 
    #print mname
    sdir     = dirname(tbzname) 
    untbzdir = fn.replace_ext(tbzname,'')        
    sname    = join(sdir,mname)    
    dstmrc   = sname + dstext
    fn.mkdir_assure(untbzdir)        
    untbz(tbzname,untbzdir,**kwargs)  
    root,srcext = splitext(glob.glob(join(untbzdir, '%s*0001.*' % mname))[0])
    #print ' Source extension %s ' % srcext
    
    gainmrc = gain2mrc(srcdir)    
    srcmics = join(untbzdir,mname) + '*'+srcext
    mrctmp  = sname + '_tmp.mrc'
    
    if srcext == '.dm4':
        dm4tomrc(srcmics,mrctmp)
    else:
        assert(srcext == '.mrc')
        stackmrcs(srcmics,mrctmp)
        # transpose gain mrc data
        transpose_mrc(gainmrc,gainmrc)
        
    # multiply all frames by gains
    # print mrctmp, gainmrc, dstmrc
    multgains(mrctmp,gainmrc,dstmrc)        
    out,err,status = sysrun('rm -rf %s' % untbzdir)
    assert(status)        
    out,err,status = sysrun('rm ' + mrctmp)
    assert(status)    
    # remove gains
    out,err,status = sysrun('rm ' + gainmrc)
    assert(status)        
    
def extract_tbz(ftbz,nth):
    mname   = fn.file_only(ftbz) 
    srcdir  = dirname(ftbz)
    # create scratch dir
    sdir    = scratch.join(SCRATCH_DIR)
    fn.mkdir_assure(sdir)    
    # ----- extract tbz file --------
    dsttbz  = join(sdir,mname) +'.tbz'    
    # copy and extract tbz
    shutil.copyfile(ftbz,dsttbz)
    # convert tbz to mrc
    tbz2mrc(srcdir,dsttbz,MOVSUFF,nthreads=nth)    
    
def write_unblur_script(dstmdir,mrcin,nth,unblurexe,nframes,angpix,do_movies,
                       dodose,dose_per_frame,vol,pre_exp):
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
    
def unblurmicro(unblurexe,sumexe,nth,ftbz,dstmdir,do_aligned_movies,
                dodose,dose_per_frame,vol,pre_exp,
                dosummovie,first_frame,last_frame):
    ''' Calls external unblur and summovie executables with corresponding params '''
    # Unblur directory in scratch
    mrcname = tbz2mrc_name(ftbz) 
    # obtain number of frames in the movie
    nframes = mrc.shape(mrcname)[0]
    angpix  = mrc.psize(mrcname)    
    # generate unblur csh script
    tprint("Running Unblur on %s" % mrcname)                
    unblur_csh = write_unblur_script(dstmdir,mrcname,nth,
                                     unblurexe,nframes,angpix,do_aligned_movies,
                                     dodose,dose_per_frame,vol,pre_exp)    
    # call unblur script
    mpi.verify(*sysrun('csh %s' % unblur_csh))  
    if dosummovie:
        # generate summovoe script
        tprint("Running Summovie on %s" % mrcname)                        
        sum_csh = write_summovie_script(dstmdir,mrcname,nth,sumexe,nframes,angpix,
                                        first_frame,last_frame)
        # call summovie script
        mpi.verify(*sysrun('csh %s' % sum_csh))  
    
def mpi_init(dstmdir,starfile):    
    '''Run by master rank 0 to initialize the processing'''
    key  = '_rlnMicrographMovieName'
    # directory for micrographs
    fn.mkdir_assure(dstmdir)   
    # read all tbz files from star file
    tbzs      = star.getlist(starfile,key)    
    if len(tbzs) == 0:
        raise(IOError('No micrographs found in %s!' % starfile))    
    return tbzs
    
def mpi_run(dstdir,unblurexe,sumexe,nth,do_aligned_movies,dodose,dosummovie,
            dose_per_frame,vol,pre_exp,first_frame,last_frame,tbz):
    ''' Run by all ranks to process a subset of elements '''
    dstmdir   = join(dstdir,MOVIE_DIR)    
    fn.mkdir_assure(dstmdir)
    if dosummovie and dodose:
        tprint('Selected subset of frames, disabling dose weighting !!!')
        dodose = False
    # process all micros in the batch        
    #for tbz in tbzgroup:
    #tprint('-'*80)
    tprint("Extracting %s ..." % (tbz))            
    # extract tbz file to movie in scratch
    extract_tbz(tbz,nth)            
    unblurmicro(unblurexe,sumexe,nth,tbz,dstmdir,do_aligned_movies,
                dodose,dose_per_frame,vol,pre_exp,
                dosummovie,first_frame,last_frame)  
    # remove uncompressed micro  
    mrcname = tbz2mrc_name(tbz)
    tprint('Removing original/uncorrected movie %s' % mrcname)
    mpi.verify(*sysrun('rm %s' % mrcname))        
          
def mpi_finish(dstdir,do_aligned_movies,tbzs):
    ''' Run by master rank 0 to finilize mpi processing '''
    # construct star files with resulting micrograph lists    
    dstmdir = join(dstdir,MOVIE_DIR)       
    tprint("Saving star files pointing to %d processed results in %s" % (len(tbzs),dstdir))     
    cmd = "relion_star_loopheader 'rlnMicrographName #1' > %saverage_micrographs.star \n \
          ls %s/*%s >> %saverage_micrographs.star" % (dstdir,dstmdir,AVGSUFF,dstdir)
    mpi.verify(*sysrun(cmd)) 
    if do_aligned_movies:
        cmd = "relion_star_loopheader 'rlnMicrographName #1 \n rlnMicrographMovieName #2' > %saligned_movies.star \n \
                ls %s/*%s >> %saligned_movies.star" % (dstdir,dstmdir,ALNSUFF,dstdir)
        mpi.verify(*sysrun(cmd)) 

def main_mpi(dstdir,starfile,unblurexe,sumexe,nth,do_aligned_movies,dodose,dosummovie,
             dose_per_frame,vol,pre_exp,first_frame,last_frame):
    if dosummovie:
        assert(last_frame >= first_frame)        
    # init scratch for each slave
    scratch.init('/scratch')    
    mpi.scatter_list(partial(mpi_init,dstdir,starfile),
                     partial(mpi_run,dstdir,unblurexe,sumexe,nth,do_aligned_movies,dodose,dosummovie,
                             dose_per_frame,vol,pre_exp,first_frame,last_frame),
                     partial(mpi_finish,dstdir,do_aligned_movies))     
    # clean scratch
    scratch.clean()                        
    
def get_parser():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@',
                                     description='Running unblur on tbz-compressed micrographs via MPI.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     epilog="Example: mpirun -n 8 `which unblur_tbz.py` -i Import/job001/movies.star -o MotionCorr/job001/" 
                                     " -j 4 -a=True -f 3 -l 20 -un /jasper/relion/Unblur/unblur_1.0.2/bin/unblur_openmp_7_17_15.exe"
                                     " -sm /jasper/relion/Summovie/summovie_1.0.2/bin/sum_movie_openmp_7_17_15.exe"
                                     "Note: if first_frame_sum and last_frame_sum specified, then dose weighting will be disabled."
                                     "Update this python script to include dose weighting via summovie utility if needed.")
    parser.add_argument('-i','--input_star_file', help='Star file with tbz-compressed filenames.', 
                        default=argparse.SUPPRESS, type=str, required=True)
    parser.add_argument('-o','--output_dir', help='Output directory', 
                        default=argparse.SUPPRESS, type=str, required=True)  
    parser.add_argument('-j','--nthreads', help='Number of threads',default = 4, type=int, required=False)                    
    #parser.add_argument('-s','--save_movies', help='Flag to save aligned movies', 
    #                    default=True, type=bool, required=False)   
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
    parser.add_argument('-un','--unblur_exe', help='Path to unblur executable.', 
                        default=argparse.SUPPRESS, type=str, required=True)
    parser.add_argument('-sm','--summovie_exe', help='Path to summovie executable.', 
                        default=argparse.SUPPRESS, type=str, required=True)
    return parser      
    
##%%##############################################


###### Main starts here #######################################    
if __name__ == "__main__":
    # Parse input and obtain all params
    args,unknown        = get_parser().parse_known_args()
    kwargs              = vars(args)
    if len(unknown) > 0:
        print "Unkown arguments %s !!! \n Quitting ..." % unknown
        quit()
    dstdir              = kwargs['output_dir']    
    starfile            = kwargs['input_star_file']
    unblurexe           = kwargs['unblur_exe']
    sumexe              = kwargs['summovie_exe']
    nth                 = kwargs['nthreads']
    do_aligned_movies   = kwargs['save_aligned_movies']
    dodose              = kwargs['do_dose']
    dose_per_frame      = kwargs['dose_per_frame']
    vol                 = kwargs['voltage']    
    pre_exp             = kwargs['pre_exp'] 
    first_frame         = kwargs['first_frame_sum']
    last_frame          = kwargs['last_frame_sum']    
    dosummovie          = last_frame != 0 or first_frame !=0
    # call main function with all params
    main_mpi(dstdir, starfile, unblurexe, sumexe, nth, do_aligned_movies, dodose, dosummovie,
             dose_per_frame, vol, pre_exp, first_frame, last_frame)
    #tprint("Align status %d" % do_aligned_movies)    
else:
    #%% ----------------- TESTS -----------------------
    starfile  = '/jasper/temp/betagal1/Import/job001/movies.star'
    dstdir    = '/jasper/temp/betagal1/MotionCorr/job001/'
    
#    starfile  = '/jasper/temp/csy.star'
#    dstdir    = '/jasper/temp/csy/'
        
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
    #%%
    scratch.init('/scratch')    
    tbzgroup = mpi_init(dstdir,starfile)
    #tbzgroup = [tbzgroup[0],tbzgroup[1]]
    tbzgroup = [tbzgroup[0]]
    
    
    #gain2mrc('/jasper/data/Livlab/projects_nih/BGal/BetaGal_PETG_20141217_2/')
    # --------------------------------------------------
    mpi_run(dstdir,unblurexe,sumexe,nth,do_aligned_movies,dodose,dosummovie,
            dose_per_frame,vol,pre_exp,first_frame,last_frame,tbzgroup[0])
#    partial(mpi_finish,dstdir,do_aligned_movies)(tbzgroup)

    
#mpirun -n 8  -hostfile ./motionhost `which unblurtbz.py` -i Import/job001/movies.star -o MotionCorr/job001/ -j 4 -s=True -f 3 -l 20 -un /jasper/relion/Unblur/unblur_1.0.2/bin/unblur_openmp_7_17_15.exe -sm /jasper/relion/Summovie/summovie_1.0.2/bin/sum_movie_openmp_7_17_15.exe

