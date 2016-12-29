# -*- coding: utf-8 -*-
"""
Created on Sun Aug 17 17:44:17 2014

@author: worker
"""

import numpy as np
from numpy.fft import fftshift,ifftshift,fft,ifft 
from numpy.fft import fft2 as fast_fft2
from numpy.fft import ifft2 as fast_ifft2
from numpy.fft import fftn as fast_fftn
from numpy.fft import ifftn as fast_ifftn
#from myutils.myplotlib import imshow
from packinon.pg_circle import calc_points 
from myutils.transformations import matrix_between_vectors 
from   skimage.measure import label #regionprops
import os
from   scipy import misc 
from   myutils.utils import tprint
from   scipy.signal import medfilt

def medfilt_ims(refs,medker,mediters):
    nrefs  = np.prod(refs.shape[:-2])
    mrefs  = np.zeros(refs.shape,np.float32)
    for k in range(nrefs):        
        mref = refs[k].copy() 
        #mref -= np.median(mref)
        #mref[mref < 0.0] = 0.0        
        for m in range(mediters):        
            mref = medfilt(mref,kernel_size=medker)
        mrefs[k] = np.float32(mref)  
    return mrefs      

def rad2res(rad,win,psize):
    return psize*win/rad  

def radfilters(N,rexp):
    nf   = rexp.size
    r    = np.arange(N,dtype=np.float32)+1.0    
    rw   = np.zeros((nf,N),dtype=np.float32)
    for k in range(nf):
        rw[k]  = np.power(r,rexp[k]) 
    #rw /= rw[:,-1][:,None]        
    return rw

def logfilters(win,nscales,minres,**kwargs):
    # filter locations
    width = kwargs.pop('width',0.33)
    N     = win//2
    minr  = int(np.round(win/minres))
    fidxs = np.int32(np.ceil(np.logspace(np.log10(minr),np.log10(N),nscales))) 
    sigs  = width*fidxs 
    x     = np.arange(N,dtype=np.float32)
    #b     = np.exp(bfact*(x**2)/(4.0*N**2))[None,:]  
    rw    = np.float32([np.exp(-((x-fidxs[k])**2)/(2.0*(sigs[k]**2))) for k in range(nscales)])
    # extend low pass
    rw[0,:fidxs[0]] = 1.0
    # extend high-pass
    rw[-1,fidxs[-1]:] = 1.0    
    #rw    = np.float32(rw/rw.sum(axis=0)[None,:]) 
    return np.ascontiguousarray(rw)

def logwidthfilters(win,nscales,**kwargs):
    # filter locations
    width = kwargs.pop('width',0.33)
    N     = win//2
    minr  = 1 #int(np.round(win/minres))
    fidxs = np.int32(np.ceil(np.logspace(np.log10(minr),np.log10(N),nscales+2))) 
    sigs  = width*(N-fidxs[1:-1]) 
    x     = np.arange(N,dtype=np.float32)
    #b     = np.exp(bfact*(x**2)/(4.0*N**2))[None,:]  
    rw    = np.float32([np.exp(-((x-N)**2)/(2.0*(sigs[k]**2))) for k in range(nscales)])
    # extend low pass
    #rw[0,:fidxs[0]] = 1.0
    # extend high-pass
    #rw[-1,fidxs[-1]:] = 1.0    
    #rw    = np.float32(rw/rw.sum(axis=1)[:,None]) 
    #rw    = np.float32(rw/rw[:,0][:,None]) 
    return np.ascontiguousarray(rw)

def pdf2mat(pdf):
    pdf   = pos(pdf)
    pdf   = pdf/pdf.sum()
    #nmats = np.prod(pdf.shape[:-2])
    sz    = pdf.shape[-2:]
    x,y   = cart_coords2D(sz)
    x,y   = x[None,:,:],y[None,:,:]
    xx    = ((x**2)*pdf).sum(axis=(-1,-2))
    yy    = ((y**2)*pdf).sum(axis=(-1,-2))
    xy    = (x*y*pdf).sum(axis=(-1,-2))    
    A     = np.float32([[[lxx,lxy],[lxy,lyy]] for [lxx,lyy,lxy] in zip(xx,yy,xy)])
    d     = [lxx*lyy-lxy*lxy for [lxx,lyy,lxy] in zip(xx,yy,xy)]
    AI    = np.float32([[[lyy/ld,-lxy/ld],[-lxy/ld,lxx/ld]] for [lxx,lyy,lxy,ld] in zip(xx,yy,xy,d)])    
    return A,AI

    
def rectmask(sz,msz):
    ''' return rectangular mask of size sz and inner size msz '''
    msk = np.ones(msz,np.float32)
    #assert(len(msz.shape)==2)
    return uncrop2D(msk,sz)

def myframe2img(frame):
    frame = frame.reformat(format='gray16le')        
    img   = np.frombuffer(frame.planes[0], np.dtype('<u2'))
    return np.reshape(img,(frame.height, img.size/frame.height))[:,:frame.width]

def video2dir(vidname,seqpath):
    import av    
    container = av.open(vidname)
    video = next(s for s in container.streams if s.type == b'video')    
    for packet in container.demux(video):
        for frame in packet.decode():        
            imname = os.path.join(seqpath,'frame-%04d.png' % frame.index)
            frame  = frame.reformat(format='gray16le')        
            img    = np.frombuffer(frame.planes[0], np.dtype('<u2'))
            im     = np.reshape(img,(frame.height, img.size/frame.height))[:,:frame.width]            
            #im  = myframe2img(frame)
            img = misc.toimage(im,high=im.max(), low=im.min(),mode='I')
            tprint('Saving %s' % (imname,))
            img.save(imname)

def props2bw(regions,sz):
    bw = np.zeros(sz, dtype='int32')
    for props in regions:
        coords = props['coords']
        idxs   = np.ravel_multi_index((coords[:,0],coords[:,1]),sz)
        bw.flat[idxs] = 1
    return bw 

def bw2coords(bw,im,min_obj_area=1.0):
    lb  = label(bw,background=0.0)
    sz  = im.shape
    x,y = np.mgrid[0:sz[0],0:sz[1]]  
    # collect detected coordinates
    coords = list()
    for l in range(1,lb.max()+1):
        # locate all pixels that belong to this index
        idxs = np.where(lb.flat == l)[0]
        if len(idxs) < min_obj_area:
            continue
        # find centroid coordinate
        ims  = im.flat[idxs].sum()
        imf  = im.flat[idxs]
        cx   = (x.flat[idxs]*imf).sum()/ims
        cy   = (y.flat[idxs]*imf).sum()/ims
        coords.append([int(cx),int(cy)])
    return coords   

def norm0_1(im):
    im -= im.min()
    return im/im.max()    
    
def norm0_255(im):
    return np.round(norm0_1(im)*255).astype(np.uint8)
    
def alpha_compose_red_green(I1,ir,ig,alpha):
    I1  = norm0_255(I1)
    I2  = I1.copy()
    I3  = I1.copy()
    nr  = ir > 0.1*ir.max()
    ng  = ig > 0.1*ig.max()
    ir  = norm0_255(ir)    
    ig  = norm0_255(ig)    
    # combine red channel
    I1[nr]  = np.uint8((1-alpha)*I1[nr] + alpha*(ir[nr]))   
    # weaken other channels 
    I2[nr] = np.uint8((1-alpha)*I2[nr])   
    I3[nr] = np.uint8((1-alpha)*I3[nr])   
    # combine green channel
    I2[ng]  = np.uint8((1-alpha)*I2[ng] + alpha*(ig[ng]))   
    # weaken other channels 
    I1[ng] = np.uint8((1-alpha)*I1[ng])   
    I3[ng] = np.uint8((1-alpha)*I3[ng])   
    return np.dstack((I1,I2,I3))   

def subim_mask(sz,subsz,xk,yk):
    sx,sy   = ndgrid(np.arange(subsz[0]),np.arange(subsz[1]))
    sx = sx + xk - subsz[0]//2
    sy = sy + yk - subsz[1]//2
    submaskx = np.logical_and(sx >= 0, sx < sz[0]) 
    submasky = np.logical_and(sy >= 0, sy < sz[1]) 
    submask  = np.logical_and(submaskx,submasky)
    #mask     = np.zeros(sz,dtype='bool')
    #mask.flat[sx[submask]*sz[1]+sy[submask]] = True
    return submask,sx,sy   

def inside(pc,sz,bsize):
    ''' Determine whether box can be picked from the micrograph '''        
    bsize2   = bsize//np.sqrt(2.0)
    res      = pc[:,0] > bsize2
    res      = np.logical_and(res,pc[:,1] > bsize2)
    res      = np.logical_and(res,pc[:,0] < sz-bsize2)
    res      = np.logical_and(res,pc[:,1] < sz-bsize2)
    return res

def mad(x):
    return np.median(np.abs(x-np.median(x)))   
    
def mad_detect(x, thresh):
    mn = np.median(x)
    md = mad(x)    
    return x > mn+thresh*md       
    
def mad_detect_masked(x, mask, thresh):
    mn = np.median(x[mask])
    md = mad(x[mask])    
    return x > mn+thresh*md    

def psize2s1d(sz,psize):
    slen = np.ceil(np.sqrt(3.0)*(sz/2.0))+1
    return np.arange(slen,dtype=np.float32)/(sz*psize)

def pa1Dfrom3D(V,psize):
    ''' 1D power amplitude from a 3D volume '''
    sz      = V.shape
    s1d     = psize2s1d(sz[0],psize)[:(sz[0]//2)]       
    VF      = np.fft.fftshift(np.abs(np.fft.fftn(V)))/np.prod(sz)
    VF      = VF**2
    # radially average
    X,Y,Z   = cart_coords3D(sz)
    r       = np.sqrt(X*X + Y*Y + Z*Z)
    rmax    = sz[0]//2
    pa      = np.zeros(rmax,dtype='float32')
    for k in range(rmax):
        pa[k] = np.sqrt(np.mean(VF[r==k]))    
    return pa,s1d

def scale2lp_res(scale):
    ''' Calculates resolution needed to filter to in order to downsize an image
        2.0 for Nyquist, 1.1 for spare '''
    return 2.0*1.0/float(scale)

def sample_sphere(ang_inc, max_lat, max_long, pole_vec=np.float32([0,0,1])):
    ''' ang_inc - angle increments, max_ang = [0,pi]'''
    points = calc_points(ang_inc, max_lat, max_long)
    points.append([0,1,0])
    points = np.array(points, dtype='float32')
    # align with z axis
    points = np.roll(points,1,axis=1)
    # transform pole
    M = matrix_between_vectors(np.float32([0,0,1]), pole_vec)
    points = np.dot(points, M[:3,:3].T)
    return points 
    
def ndgrid(x,y):
    dtype = x.dtype
    y,x   = np.meshgrid(y,x)
    return np.array(x,dtype=dtype),np.array(y,dtype=dtype)

def cart_coords2D(sz):
    #x,y = ndgrid(np.arange(sz[0])-sz[0]//2, np.arange(sz[1])-sz[1]//2)
    #ndgrid(np.arange(sz[0])-sz[0]//2, np.arange(sz[1])-sz[1]//2)
    sz  = np.int32(sz)    
    x,y = np.mgrid[0:sz[0],0:sz[1]] 
    return np.float32(x-sz[0]//2),np.float32(y-sz[1]//2) 

def cart_coords3D(sz):
    x,y,z = np.mgrid[0:sz[0],0:sz[1],0:sz[2]] 
    return np.float32(x-sz[0]//2),np.float32(y-sz[1]//2),np.float32(z-sz[2]//2)
    
def center_mass3D(V):
    sz    = V.shape
    sV    = V.sum()
    x,y,z = cart_coords3D(sz)
    return (x*V).sum()/sV,(y*V).sum()/sV,(z*V).sum()/sV
    
def roll_nD(V,shift):
    for d in range(V.ndim):
        V = np.roll(V,shift[d],axis=d)
    return V

def tile2D(sz, win_sz, ovlp, **kwargs):
    ''' Creates a list of sub-images with overlaps '''   
    marg    = np.int32(kwargs.pop('marg', 0.0))
    win_sz  = np.array(win_sz)
    ovlp_sz = np.ceil(np.float32(ovlp)*win_sz)
    xstep   = win_sz[0]-ovlp_sz[0]
    ystep   = win_sz[1]-ovlp_sz[1]
    xmarg   = ((sz[0] % xstep) - min(0,ovlp_sz[0]))//2
    ymarg   = ((sz[1] % xstep) - min(0,ovlp_sz[1]))//2

    if (xmarg < marg) | (ymarg < marg):
        xpos,ypos = tile2D(np.array(sz)-2*marg, win_sz, ovlp, marg=0)
        xpos+=marg
        ypos+=marg
        return xpos,ypos
    
    xpos    = np.arange(xmarg, sz[0]-win_sz[0]+1, xstep, dtype=np.int32)
    ypos    = np.arange(ymarg, sz[1]-win_sz[1]+1, ystep, dtype=np.int32)
    return xpos,ypos                  
    
def sub_image2D(im,**kwargs): #xpos, ypos, win_sz):
    ''' return a sub-image with upper left cornet at xpos, ypos and size win_sz'''
    xpos  = np.int32(np.round(kwargs['xpos']))  
    ypos  = np.int32(np.round(kwargs['ypos']))
    win   = np.int32(kwargs['win'])
    nf    = int(np.prod(im.shape[:-2]))
    nim   = len(xpos)
    sim   = np.empty((nf,nim,win,win),im.dtype)
    for k in range(nim):
        x,y = xpos[k],ypos[k]
        sim[...,k,:,:] = im[...,x:(x+win), y:(y+win)]
    return sim
    #return [np.ascontiguousarray(im[...,x:(x+win), y:(y+win)]) for x,y in zip(xpos,ypos)]    
    #xpos,ypos = round(xpos),round(ypos)
    #return  np.copy(im[xpos:(xpos+win_sz[0]), ypos:(ypos+win_sz[1])])       
    
def low_pass_mask1D(sz,lp_res,**kwargs):
    #shape = kwargs.pop('shape','gaussian')
    shape = kwargs['shape']
    #yy,xx = np.meshgrid(np.arange(sz[1], dtype='float32'), np.arange(sz[0], dtype='float32'))
    xx = np.arange(sz, dtype='float32')
    # center coordinates    
    xx = np.float32(xx) - sz//2
    if shape == 'box':
        return np.float32((xx*lp_res/sz) < 1.0)     
    else:
        if shape == 'gaussian':
            return np.exp(-0.5*((np.pi*xx*lp_res/sz)**2)) 
        else: raise ValueError("Unknown shape %s" % shape)
    
def low_pass_mask2D(sz,lp_res,**kwargs):
    shape = kwargs['shape']
    #xx,yy = ndgrid(np.arange(sz[0], dtype='float32'), np.arange(sz[1], dtype='float32'))
    xx,yy = np.mgrid[0:sz[0],0:sz[1]] 
    # center coordinates    
    xx = np.float32(xx) - sz[0]//2
    yy = np.float32(yy) - sz[1]//2
    if shape == 'box':
        d = np.maximum((xx*lp_res/sz[0])**2,(yy*lp_res/sz[1])**2)
        return np.float32(d<1.0)
        #return np.float32( ((xx*lp_res/sz[0])**2 + (yy*lp_res/sz[1])**2) < 1.0)
    else:
        if shape == 'gaussian':
            return np.exp(-0.5*((np.pi*xx*lp_res/sz[0])**2 + (np.pi*yy*lp_res/sz[1])**2))  
        else: raise ValueError("Unknown shape %s" % shape)
        
def low_pass_mask3D(sz,lp_res,**kwargs):
    shape = kwargs['shape']
    xx,yy,zz = np.mgrid[0:sz[0],0:sz[1],0:sz[2]]                   
    # center coordinates    
    xx = np.float32(xx) - sz[0]//2
    yy = np.float32(yy) - sz[1]//2
    zz = np.float32(zz) - sz[2]//2
    if shape == 'box':
        d = np.maximum((xx*lp_res/sz[0])**2,(yy*lp_res/sz[1])**2)
        d = np.maximum(d,(zz*lp_res/sz[2])**2)
        return np.float32(d < 1.0)
        #return np.float32( ((xx*lp_res/sz[0])**2 + (yy*lp_res/sz[1])**2 + (zz*lp_res/sz[2])**2) < 1.0)
    else:
        if shape == 'gaussian':
            return np.exp(-0.5*((np.pi*xx*lp_res/sz[0])**2+
                    (np.pi*yy*lp_res/sz[1])**2+(np.pi*zz*lp_res/sz[2])**2))         
        else: raise ValueError("Unknown shape %s" % shape)    
    
def low_pass_mask_half1D(sz,low_res,**kwargs):
    mask = low_pass_mask1D(sz,low_res,**kwargs)
    mask = ifftshift(mask)
    return np.copy(mask[0:sz[1]//2+1])    
    
def low_pass_mask_half2D(sz,low_res,**kwargs): 
    mask = low_pass_mask2D(sz,low_res,**kwargs)
    mask = ifftshift(mask)
    return fft_full2half2D(mask)   
    
def rad_half2D(sz):
    x,y = cart_coords2D(sz)  
    r   = ifftshift(np.sqrt(x**2+y**2))
    return fft_full2half2D(r)

#    
def rad2D_spat(sz):    
    x,y = cart_coords2D(sz)  
    return np.sqrt(x**2+y**2)
        
def rad_half3D(sz):
    x,y,z = cart_coords3D(sz)  
    r     = ifftshift(np.sqrt(x**2+y**2+z**2))
    return fft_full2half3D(r)  
    
def rad3D_freq(sz):
    x,y,z = cart_coords3D(sz)  
    r     = ifftshift(np.sqrt(x**2+y**2+z**2))
    return r        
    
def circ_mask2D(sz,R):
    x,y = cart_coords2D(sz)  
    r   = np.sqrt(x**2+y**2)
    return np.float32(r <= R)
    
def circ_mask3D(sz,R):
    x,y,z = cart_coords3D(sz)  
    r   = np.sqrt(x**2+y**2+z**2)
    return np.float32(r <= R)
                
def low_pass_mask_half3D(sz,low_res,**kwargs):
    mask = low_pass_mask3D(sz,low_res,**kwargs)
    mask = ifftshift(mask)
    return fft_full2half3D(mask)     
        
def fft_full2half2D(imf):
    sz = imf.shape[-2:]
    return np.copy(imf[...,:sz[1]//2+1])   

def fft_full2half3D(Vf):
    sz = Vf.shape[-3:]
    return np.copy(Vf[...,:sz[2]//2+1])  
    
def crop2D_fft_half(im,sz): 
    ''' Crops im to match sz '''
    imsz    = im.shape[-2:]
    nframes = int(np.prod(im.shape[:-2]))
    assert(imsz[0] >= sz[0])
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)    
    imc     = np.zeros((nframes,sz[0],sz[1]//2+1),dtype=im.dtype)
    # copy redundant part
    imc[...,:(sz[0]//2),:sz[1]//2]  = im[...,:(sz[0]//2),:sz[1]//2]
    imc[...,-(sz[0]//2)+1:,:sz[1]//2] = im[...,-(sz[0]//2)+1:,:sz[1]//2]
    # convert middle frequency part 
    #lastline = np.roll(np.conj(im[...,::-1,sz[1]//2]),1,axis=-1)
    ## include nonredundant one
    #imc[...,:(sz[0]//2),-1]  = lastline[...,:(sz[0]//2)]
    #imc[...,-(sz[0]//2):,-1] = lastline[...,-(sz[0]//2):]    
    return np.reshape(imc, im.shape[:-2] + (sz[0],sz[1]//2+1))    
    
def uncrop2D_fft_half(imc,sz): 
    ''' Crops im to match sz '''
    imsz    = list(imc.shape[-2:])
    imsz[1] = 2*(imsz[1]-1)
    nframes = int(np.prod(imc.shape[:-2]))
    assert(imsz[0] <= sz[0])
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)    
    imu     = np.zeros((nframes,sz[0],sz[1]//2+1),dtype=imc.dtype)
    # copy redundant part
    imu[...,:(imsz[0]//2),:imsz[1]//2]  = imc[...,:(imsz[0]//2),:imsz[1]//2]
    imu[...,-(imsz[0]//2)+1:,:imsz[1]//2] = imc[...,-(imsz[0]//2)+1:,:imsz[1]//2]    
    # no nonredundant part in the uncrop
    return np.reshape(imu, imc.shape[:-2] + (sz[0],sz[1]//2+1))        
    
def crop3D_fft_half(im,sz): 
    ''' Crops im to match sz '''
    imsz    = im.shape[-3:]
    nframes = np.prod(im.shape[:-3])
    assert(imsz[0] >= sz[0])
    # not built to rescale uneven sizes
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)
    imc     = np.zeros((nframes,sz[0],sz[1],sz[2]//2+1),dtype=im.dtype)
    imc[...,:(sz[0]//2),:sz[1]//2,:sz[2]//2] = im[...,:(sz[0]//2),:sz[1]//2,:sz[2]//2]
    imc[...,-(sz[0]//2)+1:,-(sz[1]//2)+1:,:sz[2]//2] = im[...,-(sz[0]//2)+1:,-(sz[1]//2)+1:,:sz[2]//2]
    imc[...,:(sz[0]//2),-sz[1]//2+1:,:sz[2]//2] = im[...,:(sz[0]//2),-sz[1]//2+1:,:sz[2]//2]
    imc[...,-(sz[0]//2)+1:,:(sz[1]//2),:sz[2]//2] = im[...,-(sz[0]//2)+1:,:(sz[1]//2),:sz[2]//2]
    #lastline = np.roll(np.conj(2.0*im[...,::-1,::-1,sz[2]//2]),1,axis=-1)
    #imc[...,:(sz[0]//2),:(sz[1]//2),-1]  = lastline[...,:(sz[0]//2),:(sz[1]//2)]
    #imc[...,-(sz[0]//2):,-(sz[1]//2):,-1] = lastline[...,-(sz[0]//2):,-(sz[1]//2):]    
    return imc    
    
def uncrop3D_fft_half(imc,sz): 
    ''' Crops im to match sz '''
    imsz     = list(imc.shape[-3:])
    imsz[-1] = 2*(imsz[-1]-1)
    nframes  = int(np.prod(imc.shape[:-3]))
    assert(imsz[0] <= sz[0])
    # not built to rescale uneven sizes
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)    
    imu      = np.zeros((nframes,sz[0],sz[1],sz[2]//2+1),dtype=imc.dtype)
    # copy redundant part
    imu[...,:(imsz[0]//2),:(imsz[1]//2),:imsz[2]//2]  = imc[...,:(imsz[0]//2),:(imsz[1]//2),:imsz[2]//2]
    imu[...,-(imsz[0]//2)+1:,-(imsz[1]//2)+1:,:imsz[2]//2] = imc[...,-(imsz[0]//2)+1:,-(imsz[1]//2)+1:,:imsz[2]//2]    
    imu[...,:(imsz[0]//2),-(imsz[1]//2)+1:,:imsz[2]//2] = imc[...,:(imsz[0]//2),-(imsz[1]//2)+1:,:imsz[2]//2]    
    imu[...,-(imsz[0]//2)+1:,:(imsz[1]//2),:imsz[2]//2] = imc[...,-(imsz[0]//2)+1:,:(imsz[1]//2),:imsz[2]//2]    
    # no nonredundant part in the uncrop
    return np.reshape(imu, imc.shape[:-3] + (sz[0],sz[1],sz[2]//2+1))        
    
    
def fft_half2full2D(imf, sz):
    ''' Transform nonredundant fft representation to full with reflected symmetry 
        After fftshift, DC frequency is located at sz[0]//2, sz[1]//2 '''
    if np.all(np.int32(sz) == np.int32(imf.shape)):
        return imf
    M2       = sz[1]//2+1
    flipIdxs = np.arange(M2-1+sz[1]%2, 1, -1)-1 
    imm      = np.roll(imf[...,::-1,flipIdxs], 1, axis=imf.ndim-2)
    return np.concatenate((imf, np.conjugate(imm)), axis=imf.ndim-1)    
    
def fft_half2full3D(imf, sz):
    ''' Transform nonredundant fft representation to full with reflected symmetry 
        After fftshift, DC frequency is located at sz[0]//2, sz[1]//2 '''
    if np.all(np.int32(sz) == np.int32(imf.shape)):
        return imf
    P2       = sz[2]//2+1
    flipIdxs = np.arange(P2-1+sz[2]%2, 1, -1)-1 
    imm      = np.roll(imf[...,::-1,::-1,flipIdxs], 1, axis=imf.ndim-3)
    imm      = np.roll(imm, 1, axis=imf.ndim-2)
    return np.concatenate((imf, np.conjugate(imm)), axis=imf.ndim-1)       
    
def low_pass_fft1D(x,lp_res,**kwargs): 
    ''' Smoothes last dimension to lp_res resolution '''
    #axis = kwargs.pop('axis',-1)
    #return gaussian_filter(im, lp_res/4)
    #sz    = np.array(im.shape)
    mask = low_pass_mask1D(x.shape[-1],lp_res,**kwargs)
    xfft = fft(x,axis=-1)
    xfft = xfft*fftshift(mask)
    return np.float32(np.real(ifft(xfft,axis=-1)))        

def low_pass_fft2D(im,lp_res,**kwargs):
    ''' Smoothes image to lp_res resolution '''
    mask  = low_pass_mask2D(im.shape,lp_res,**kwargs)
    imfft = fast_fft2(im)*ifftshift(mask)
    # here ifft2 would always convert complex64 to coplex 128, so castging
    # to float32 is needed
    return np.float32(np.real(fast_ifft2(imfft))).copy()
    
def low_pass_fft3D(V,lp_res,**kwargs):
    ''' Smoothes image to lp_res resolution '''
    mask = low_pass_mask3D(V.shape,lp_res,**kwargs)    
    Vfft = fast_fftn(V)*ifftshift(mask)
    # here ifft2 would always convert complex64 to coplex 128, so castging
    # to float32 is needed
    return np.float32(np.real(fast_ifftn(Vfft))).copy()
        
def crop2D(im,sz,x=None,y=None):
    ''' Crops im to match sz '''
    imsz   = np.array(im.shape[-2:])
    assert(imsz[0] >= sz[0])
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)    
    if (x is None) or (y is None):
        x,y = imsz[0]//2,imsz[1]//2
    posl = np.int32([x-sz[0]//2,y-sz[1]//2])
    #posr = [x+sz[0]//2,y+sz[1]//2] 
    posr = posl + np.int32(sz) # [x+sz[0]//2,y+sz[1]//2]     
    return np.copy(im[...,posl[0]:posr[0],posl[1]:posr[1]]) 
    
def crop3D(im,sz,x=None,y=None,z=None):
    ''' Crops im to match sz '''
    imsz   = np.array(im.shape[-3:])
    if imsz[0] == sz[0]:
        return im
    assert(imsz[0] >= sz[0])
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)    
    if (x is None) or (y is None) or (z is None):
        x,y,z = imsz[0]//2,imsz[1]//2,imsz[2]//2
    posl = [x-sz[0]//2,y-sz[1]//2,z-sz[2]//2] 
    posr = posl + np.int32(sz) # [x+sz[0]//2,y+sz[1]//2]     
    #posr = [x+sz[0]//2,y+sz[1]//2,z+sz[2]//2] 
    return np.copy(im[...,posl[0]:posr[0],posl[1]:posr[1],posl[2]:posr[2]]) 
    
def make_cubic(V):
    ''' Uncrops volume to cubic shape of even size '''
    sz      = V.shape
    sz      = np.int32(sz)
    sz      = sz - np.mod(sz,2)
    V       = V[:sz[0],:sz[1],:sz[2]]
    sz      = V.shape
    mxsz    = max(sz)
    return uncrop3D(V,(mxsz,)*3)    

def combine_fft(Vl,Vs):
    ''' Replaces low-freq information in Vl by Vs '''
    if Vl.shape == Vs.shape:
        return Vs    
    lsz    = np.int32(Vl.shape)
    ssz    = np.int32(Vs.shape)  
    unpadl = (lsz-ssz)//2
    unpadr = lsz - (ssz + unpadl)
    # fft transform both volumes
    Vlf    = np.fft.fftn(Vl)
    Vsf    = np.fft.fftn(Vs)
    # remove dc
    Vlf[0,0,0] = 0
    Vsf[0,0,0] = 0
    Vlf    = np.fft.fftshift(Vlf)
    Vsf    = np.fft.fftshift(Vsf)
    # replace info
    Vlcrop = Vlf[unpadl[0]:-unpadr[0], 
                 unpadl[1]:-unpadr[1],
                 unpadl[2]:-unpadr[2]].copy()                   
    vlc   = np.concatenate((np.real(Vlcrop).flatten(),np.imag(Vlcrop).flatten()))
    vsf   = np.concatenate((np.real(Vsf).flatten(),np.imag(Vsf).flatten()))
    coef  = np.dot(vlc,vsf)/np.dot(vsf,vsf)  
    # plug frequency info
    Vlf[unpadl[0]:-unpadr[0], 
        unpadl[1]:-unpadr[1],
        unpadl[2]:-unpadr[2]] = coef*Vsf
    Vn = np.fft.ifftn(np.fft.ifftshift(Vlf))    
    return np.float32(np.real(Vn))
    
def match_size3D(im,sz):
    if im.shape[-3] < sz[0]:
        im = uncrop3D(im,sz)
    else:
        im = crop3D(im,sz)
    return im  
    
def match_size2D(im,sz):
    if im.shape[-2] < sz[0]:
        im = uncrop2D(im,sz)
    else:
        im = crop2D(im,sz)
    return im      

def background_remove1D(x,lp_res,**kwargs):
    ''' Removes low-pass background along last dimension '''
    l      = x.shape[-1]
    pad    = np.int32(np.minimum(2.0*lp_res, l/3.0))
#    x      = np.pad(x, ((0,0),(0,0),(0,0),(pad,pad)), mode='symmetric')
    x      = np.lib.arraypad.pad(x, ((0,0),(0,0),(0,0),(pad,pad)), mode='reflect')
    x      = x - low_pass_fft1D(x,lp_res,**kwargs)
    return np.copy(x[...,pad:-pad])          
    
def background_remove2D(im,lp_res): 
    ''' Removes low-pass background '''
    sz    = np.array(im.shape)
    pad   = int(np.minimum(2.0*lp_res, sz[0]/3.0))
    # pad image for avoiding edge-effects
#    im = np.pad(im, ((pad,pad),(pad,pad)), mode='symmetric')
    im = np.lib.arraypad.pad(im, ((pad,pad),(pad,pad)),mode='reflect')
    im -= low_pass_fft2D(im,lp_res,shape='gaussian')
    return crop2D(im, sz) 
         
def decimate2D(im, bn):
    ''' decimate images alog two last dimenstions '''
    shape = im.shape
    sz    = list(shape[-2:]) 
    n_frames = np.prod(shape[:-2])
    im = np.reshape(im, [n_frames] + sz)
    im = im[:, 0:sz[0]:bn, 0:sz[1]:bn];
    im = np.reshape(im, shape[:-2]+im.shape[-2:])
    return np.copy(im)
    
def decimate3D(im, bn):
    ''' decimate images alog two last dimenstions '''
    shape = im.shape
    sz    = list(shape[-3:]) 
    n_frames = np.prod(shape[:-3])
    im = np.reshape(im, [n_frames] + sz)
    im = im[:, 0:sz[0]:bn, 0:sz[1]:bn,0:sz[2]:bn];
    im = np.reshape(im, shape[:-3]+im.shape[-3:])
    return np.copy(im)       
                
def scale2D(im,newsz,**kwargs):
    sz = im.shape
    # the new size has to be even
    assert(np.mod(newsz[0],2)==0)
    if sz[0] == newsz[0]:
        return im.copy()   
    imf = np.fft.fftn(im)
    imf = np.fft.fftshift(imf)   
    if newsz[0] > sz[0]:
        imf = uncrop2D(imf,newsz)
    else:
        imf = crop2D(imf,newsz)
    imf = np.fft.ifftshift(imf)
    return np.float32(np.real(np.fft.ifftn(imf)))*np.prod(newsz)/np.prod(sz) 
    
def uncrop2D(im,sz): #!!
    imsz   = np.int32(im.shape[-2:])
    assert(imsz[0] <= sz[0])
    if imsz[0] == sz[0]:
        return im
        
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)
        
    unpadl = (sz-imsz)//2 + np.mod(sz-imsz,2)
    unpadr = sz - (imsz + unpadl)
    imu    = np.zeros(im.shape[:-2]+tuple(sz),dtype=im.dtype)
    imu[...,unpadl[0]:sz[0]-unpadr[0], unpadl[1]:sz[1]-unpadr[1]] = im 
    return imu    
    
def uncrop3D(im,sz):
    imsz   = np.array(im.shape[-3:])
    assert(imsz[0] <= sz[0])
    if imsz[0] == sz[0]:
        return im    
#    print imsz[0],sz[0]
    assert(np.mod(imsz[0],2)==0)
    assert(np.mod(sz[0],2)==0)        
    unpadl = (sz-imsz)//2 + np.mod(sz-imsz,2)
    unpadr = sz - (imsz + unpadl)
    imu    = np.zeros(im.shape[:-3]+tuple(sz),dtype=im.dtype)
    imu[...,unpadl[0]:sz[0]-unpadr[0], unpadl[1]:sz[1]-unpadr[1], unpadl[2]:sz[2]-unpadr[2]] = im 
    return imu    
    
def median_replace(im,bp,neib=5):
    #neib  = 3 # width of the correction neighborhood 
    shape = im.shape
    if bp:
        for x,y in zip(bp[0],bp[1]):
            win     = np.copy(im[max(0,x-neib):min(shape[0],x+neib), \
                              max(0,y-neib):min(shape[1],y+neib)])
            im[x,y] = np.median(win) #.median()    
    return im    
    
def norm_min_max(ims):
    ''' normalize image values to lie in [0,1] '''
    nd  = np.ndim(ims)
    mx  = ims.max(axis=(nd-2,nd-1))   
    mn  = ims.min(axis=(nd-2,nd-1))    
    if nd > 2:  
        mx = mx[:,None,None] 
        mn = mn[:,None,None] 
    ims = (ims - mn)/(mx-mn)    
    ims = np.minimum(ims,1)
    ims = np.maximum(ims,0)
    return ims
    
def norm_l2(vecs):
    norms = np.sqrt(np.sum((vecs**2),axis=1))[:,None]
    return vecs/norms

def outlieridxs(im,thresh):
    #nn     = np.isnan(im)
    #im[nn] = 0 # have to get rid of nan
    #s      = im.std()
    mn     = np.median(im) #im.mean()
    s      = np.median(np.abs(im-mn))
    #bp     = np.nonzero(np.logical_or(nn, np.abs(im-mn) > thresh*s))        
    bp     = np.nonzero(np.abs(im-mn) > thresh*s)        
    return bp    
    
def unbad2D(im,thresh=10,neib=5):
    oidxs = outlieridxs(im,thresh)
    return median_replace(im,oidxs,neib)
        
def flatten(im):
    nframes = np.prod(im.shape[:-2])
    return np.reshape(im, [nframes] + list(im.shape[-2:]))
    
def imsize(im):
    return im.shape[-2:] 
    

def stack2vecs(stack,msk):
    nframes = stack.shape[0]
    pin     = np.zeros((nframes,msk.sum()),np.float32)
    for f in range(nframes):
       pin[f,:]  = stack[f][msk]
    return pin
        
def histeq(im):
   return np.sort(im.ravel()).searchsorted(im)
    
def montage(I, nn=None, **kwargs):   
    I  = flatten(I)
    count,m,n = I.shape   
    bm = kwargs.pop('margin', m//80)
    bn = kwargs.pop('margin', n//80)
    if nn is None:
        nn = int(np.ceil(np.sqrt(count)))
        mm = int(np.ceil(np.float32(count)/nn))
    else: 
        mm = int(np.ceil(np.float32(count)/nn))        
    M  = np.ones([mm*(m+2*bm), nn*(n+2*bn)], dtype='float32')*I.mean()
    #print nn, mm, count
    image_id = 0
    for j in range(mm):
        for k in range(nn):
            if image_id >= count: 
                break
            sliceM, sliceN = j*(m+2*bm), k*(n+2*bn)
            #print sliceM+m+bm
            M[sliceM+bn:sliceM+n+bn, sliceN+bm:sliceN+m+bm] = I[image_id, :, :]
            image_id += 1    
    return M
    
def pos(im):
    imout = im.copy()
    imout[im<0]=0
    return imout
 
def bin2D_fft(im,**kwargs):    
    ''' Binning with any bin value > 1 using fft interpolation. '''
    bn     = kwargs.pop('bin',None)
    cropsz = kwargs.pop('dst_sz',None)
    sz     = np.int32(im.shape[-2:])
    # these params are mutually exclusive
    assert((bn != None) ^ (cropsz != None))
    if bn != None:
        cropsz = np.int32(np.round(sz/bn))        
        cropsz = (cropsz - np.mod(cropsz,2)).tolist()
    # make size evene
    imf    = np.fft.fft2(im)
    imf    = np.fft.fftshift(imf,axes=(-1,-2))
    imf    = crop2D(imf,cropsz)
    imf    = np.fft.ifftshift(imf,axes=(-1,-2))
    imb    = np.float32(np.real(np.fft.ifft2(imf)))
    return imb*np.sqrt(np.float32(np.prod(cropsz))/np.float32(np.prod(sz))) 
    
def scale3D(V,newsz,**kwargs):
    sz = V.shape
    # the new size has to be even
    assert(np.mod(newsz[0],2)==0)
    if sz[0] == newsz[0]:
        return V    
    Vf = np.fft.fftn(V)
    Vf = np.fft.fftshift(Vf)   
    if newsz[0] > sz[0]:
        Vf = uncrop3D(Vf,newsz)
    else:
        Vf = crop3D(Vf,newsz)
    Vf = np.fft.ifftshift(Vf)
    return np.float32(np.real(np.fft.ifftn(Vf)))*np.prod(newsz)/np.prod(sz)    
    
        
##################GARBAGE################################  
#    x,y,z  = sz[0]//2,sz[1]//2,sz[2]//2
#    posl   = [x-imsz[0]//2,y-imsz[1]//2,z-imsz[2]//2] 
#    posr   = [posl[0]+imsz[0],posl[1]+imsz[1],posl[2]+imsz[2]] 
#
#    imu    = np.zeros(im.shape[:-3] + tuple(sz),dtype=im.dtype)
#    imu[...,posl[0]:posr[0],posl[1]:posr[1],posl[2]:posr[2]] = im             
#    return imu      
        
#def uncrop1D(V,sz):
#    oldsz  = V.shape[-1]    
#    x      = sz//2
#    posl   = x-oldsz//2
#    posr   = posl+oldsz 
#    Vu    = np.zeros(V.shape[:-1] + (sz,),dtype=V.dtype)
#    Vu[...,posl:posr] = V             
#    return Vu          
        
#def scale1D(V,newsz,**kwargs):
#    sz = V.shape[-1]
#    # the new size has to be even
#    assert(np.mod(newsz,2)==0)
#    if sz == newsz:
#        return V    
#    Vf = np.fft.fft(V,axis=-1)
#    Vf = np.fft.fftshift(Vf,axes=(-1,))   
#    #print sz,newsz
#    if newsz > sz:
#        Vf = uncrop1D(Vf,newsz).copy()
#        #Vf = crop3D(Vf,sz)
#    else:
#        assert(0)
#    Vf = np.fft.ifftshift(Vf,axes=(-1,))
#    return np.float32(np.real(np.fft.ifftn(Vf)))
##    if scale < 1.0:
##        V = low_pass_fft3D(V, scale2lp_res(scale),**kwargs)
###        im = low_pass_gausND(im,ANTIALIAS/float(scale),dim)
##    return zoom(V,scale)       

#def apodize_fun3D(M,apod_fact = 1e-6):
#    ''' calculates weight that drop off with the distance from mask '''
#    D   = calc_dist_map(M)
#    mxd = D.max()/np.sqrt(3.0)
#    # apply gaussian that equals apod_fact at max dist
#    sig2=-(mxd**2)/(2*np.log(apod_fact))        
#    return np.exp(-(D**2)/(2*sig2))     

#def stack2vecs12(stack,msk):
#    nframes = stack.shape[0]
#    Min     = msk==2.0
#    Mout    = msk==1.0
#    pin     = np.zeros((nframes,Min.sum()),np.float32)
#    pout    = np.zeros((nframes,Mout.sum()),np.float32)
#    for f in range(nframes):
#       pin[f,:]  = stack[f][Min]
#       pout[f,:] = stack[f][Mout]
#    return pin,pout    
        #def masks2fgbg(msks,sz,):
#    # rescle and find foreground and background in masks
#    nstacks = np.prod(msks.shape[:-2])
#    corners = circ_mask(sz,sz[0]//2) == 0.0    
#    cmsks   = np.zeros((nstacks,) + tuple(sz),np.float32)
#    for k in range(nstacks):
#        msk = scale2D(msks[k],sz)
#        msk[msk > 0.5] = 2.0 # foreground
#        msk[msk < 0.5] = 0.0 # background
#        msk[corners]   = 1.0 # dead zone        
#        cmsks[k] = np.ascontiguousarray(msk)            
#    return cmsks  

#def water_ring_mask(wrgs,win,psize): 
#    ''' generate frequency mask that leaves water rings only '''
#    x,y  = cart_coords2D([win,win])
#    r    = np.sqrt(x**2+y**2)
#    rs   = rad2res(r,win,psize)
#    mask = np.zeros((win,win),np.float32) 
#    for wr in wrgs:
#        mask[np.logical_and(rs < wr[0],rs >= wr[1])] = 1.0
#    return mask   
#
#def gen_ellipses(sz,axes,vecs,ellip_rot):
#    ''' Generate a set of ellipse sections given
#        sz      - window size
#        vecs    - normals to section planes
#        ellip_rot  - rotation of the resulting ellipses '''
#    A = np.float32([[axes[0],0,0],[0,axes[1],0],[0,0,axes[2]]])    
#    nvecs = vecs.shape[0]     
#    x,y   = cart_coords2D(sz)    
#    # rotate coordiantes
#    xr    = np.cos(ellip_rot)*x + np.sin(ellip_rot)*y
#    yr    = -np.sin(ellip_rot)*x + np.cos(ellip_rot)*y        
#    ells  = list()           
#    for k in range(nvecs):
#        # rotates [0,0,1] to point
#        # rotation of z axis to the point
#        M  = matrix_between_vectors(np.float32([0,0,1]), vecs[k,:])
#        # apply rotation to the scaled axes
#        MA = np.dot(M[:3,:3],A)
#        # determine major and minor axes in the resulting xy projection
#        S = np.linalg.svd(MA[:2,:],full_matrices=False,compute_uv=False)     
#        r2 = ((2*xr/S[0])**2 + (2*yr/S[1])**2)
#        r2[r2<1.0] = 1.0        
#        # contruct ellipsoid
#        ells.append(np.float32((2*xr/S[0])**2 + (2*yr/S[1])**2 <= 1.0))
#        #ells.append(np.exp(-((2*xr/S[0])**2 + (2*yr/S[1])**2)))
##        ells.append(np.exp(-r2**2)) # 
#    return ells  
#def test_tile2D(sz, win_sz, ovlp, marg=0):
#    xpos, ypos = tile2D(sz, win_sz, ovlp, marg)
#    im = np.zeros(sz, dtype='float32')
#    for x in range(xpos.shape[0]):
#        for y in range(ypos.shape[0]):
#            im[xpos[x]:(xpos[x]+win_sz[0]), ypos[y]:(ypos[y]+win_sz[1])] = 1.0
#    imshow(im)         

#def tile_image2D(im, win_sz, ovlp):
#    xpos, ypos = tile2D(im.shape, win_sz, ovlp)
#    tiles      = []
#    for x in range(xpos.shape[0]):
#        for y in range(ypos.shape[1]):
#            imt = sub_image2D(im, xpos[x], ypos[y], win_sz)
#            tiles.append(imt)
#    return tiles     
#def im2stack(im,xpos,ypos,wsz):
#    nims  = xpos.size
#    stack = np.zeros((nims,)+tuple(wsz), dtype='float32')  
#    for f in range(nims):
#        stack[f] = sub_image2D(im,xpos.flat[f],ypos.flat[f],wsz)  
#    return stack         
#def autocorr3D(V):
#    VF = np.fft.fftn(V)
#    VF = VF*np.conj(VF)
#    A  = np.float32(np.real(np.fft.ifftn(VF)))
#    return np.fft.fftshift(A)
         
         
#def fft2_nz(im, **kwargs):
#    ''' calculates nonuniform fft2 by ignoring zero image values
#        usage:  fft2_nz(im, freq_width=0.3, stop_thresh=1e-1)
#                freq_width in [0, 0.5] - frequency regularization width
#                low value - higher regularization, default = 0.3
#                stop_thresh specifies when to stop optimization iterations
#                default - 1e-1 '''
#    if 'freq_width' in kwargs:
#        fwidth = kwargs.get('freq_width')
#    else:
#        fwidth = 0.3
#    if 'stop_thresh' in kwargs:
#        tol = kwargs.get('stop_thresh')
#    else:
#        tol = 0.3   
#    im      = fftshift(im)         
#    N       = im.shape[0]
#    nz      = np.nonzero(im)
#    x       = np.double(np.vstack(nz).T)/N - 0.5
#    M       = x.shape[0]
#    xx      = np.linspace(-0.5, 0.5*(N-1)/N, N)
#    yy      = np.linspace(-0.5, 0.5*(N-1)/N, N)
#    xx,yy   = np.ndgrid(xx,yy)
#    # radial weight that penalizes high frequencies
#    w_hat   = np.exp(-(xx**2 + yy**2)/(fwidth**2))
#    # flags=('PRE_PHI_HUT', 'PRE_PSI') PRE_FG_PSI
#    this_nfft   = NFFT(N=[N,N], M=M, flags=('PRE_PHI_HUT', 'PRE_FG_PSI')) 
#    this_nfft.x = x
#    this_nfft.precompute()
#    #ret2        = this_nfft.adjoint()
#    #'PRECOMPUTE_WEIGHT'
#    this_solver = Solver(this_nfft, flags=('CGNR', 'PRECOMPUTE_DAMP')) 
#    this_solver.y = im[nz]
#    this_solver.w_hat = w_hat    
#    # weighting factors
#    #this_solver.w = im[nz]
#    # initialize solver internals
#    this_solver.before_loop()       
#
#    while not np.all(np.abs(this_solver.r_iter) < tol):
#        this_solver.loop_one_step()    
#        
#    imnfft = ifftshift(this_solver.f_hat_iter)
#    imnfft = N*N*np.conj(imnfft)
#    return imnfft    
         
         
#def fix_axes_3D(V):
#    ### fix central lines point ###
#    cp = V.shape[0]//2
#    V[:,cp,cp] = 0.0
#    V[cp,:,cp] = 0.0
#    V[cp,cp,:] = 0.0
#    for dx in [-1,0,1]:
#        for dy in [-1,0,1]:
#            if dx !=0 or dy != 0:
#                V[:,cp,cp] += V[:,cp+dx,cp+dy]/8.0
#                V[cp,:,cp] += V[cp+dx,:,cp+dy]/8.0
#                V[cp,cp,:] += V[cp+dx,cp+dy,:]/8.0
#    V[cp,cp,cp] /= 3.0*6.0/8.0
#    return V             
         
#def bin2D_dec(im,bn,**kwargs):   
#    ''' binning with powers of 2 using decimation ''' 
#    # Gaussian width factor for antialising low pass
#    #im = low_pass_gausND(im,ANTIALIAS*bn,2)
#    #im = gaussian_filter(im, 1.5*bn/2)
#    im = low_pass_fft2D(im,scale2lp_res(1.0/bn),**kwargs)
#    return decimate2D(im, bn)  
#
         
         
#def calc_dist_map(W):
#    # maximum manageable size
#    max_sz = 96.0
#    sz = W.shape
#    if sz[0] > max_sz:
#        # shrink the mask
#        W = decimate3D(W,float(sz[0])/max_sz)    
#    D = np.float32(skfmm.distance(W==0))    
#    if sz[0] > max_sz:
#        # restore size
#        D = scale3D(D,sz)      
#    return D
    
#def background_remove2D_pyramid(im,lp_res):
#    ''' A more efficient implementation memory-wise '''
#    ''' Removes low-pass background '''
#    sz      = np.float32(np.array(im.shape))
#    mn,mx   = im.min(),im.max()
#    # pyramid needs values in [-1 1]    
#    im      = (im-mn)/(mx-mn)
#    pyramid = tuple(pyramid_gaussian(im, downscale=2))
#    pyridx  = int(np.floor(np.log2(float(lp_res))))
#    szp     = np.float32(pyramid[pyridx].shape)
#    imd     = np.float32(pyramid[pyridx])
#    return (im - zoom(imd,(sz[0]/szp[0],sz[1]/szp[1])))*(mx-mn) + mn
    
#def downsize2D_pyramid(im,scale):
#    ''' Downsize image efficient in memory '''
#    lp_res  = 2.0/float(scale)
#    # pyramid needs values in [-1 1]
#    mn,mx   = im.min(),im.max()
#    im      = (im-mn)/(mx-mn)
#    pyramid = tuple(pyramid_gaussian(im, downscale=2))
#    pyridx  = int(np.floor(np.log2(float(lp_res))))
#    res     = np.float32(pyramid[pyridx])*(mx-mn) + mn
#    del pyramid
         
#def apodize_fun2D(W,apod_fact = 1e-6):
#    ''' calculates weight that drop off with the distance from mask '''    
#    D   = calc_dist_map(W)
#    mxd = D.max()/np.sqrt(2.0)
#    # apply gaussian that equals apod_fact at max dist
#    sig2=-(mxd**2)/(2*np.log(apod_fact))        
#    return np.exp(-(D**2)/(2*sig2))           
            
#def fix_axis3D(im):
#    sz = im.shape
#    # average x axis 
#    im[:,sz[1]//2,sz[2]//2] = (im[:,sz[1]//2-1,sz[2]//2]+
#                            im[:,sz[1]//2+1,sz[2]//2]+
#                            im[:,sz[1]//2,sz[2]//2-1]+
#                            im[:,sz[1]//2,sz[2]//2+1])/4.0 
##    # average y axis                        
##    im[sz[0]//2,:,sz[2]//2] = (im[sz[0]//2-1,:,sz[2]//2]+
##                            im[sz[0]//2+1,:,sz[2]//2]+
##                            im[sz[0]//2,:,sz[2]//2-1]+
##                            im[sz[0]//2,:,sz[2]//2+1])/4.0                            
#    return im             
         
#def fix_center2D(im):
#    sz = im.shape
#    im[sz[0]//2,sz[1]//2] = (im[sz[0]//2-1,sz[1]//2]+
#                            im[sz[0]//2+1,sz[1]//2]+
#                            im[sz[0]//2,sz[1]//2-1]+
#                            im[sz[0]//2,sz[1]//2+1])/4.0
#    return im
#
#def fix_center3D(im):
#    sz = im.shape
#    im[sz[0]//2,sz[1]//2,sz[2]//2] = (im[sz[0]//2-1,sz[1]//2,sz[2]//2]+
#                            im[sz[0]//2+1,sz[1]//2,sz[2]//2]+
#                            im[sz[0]//2,sz[1]//2-1,sz[2]//2]+
#                            im[sz[0]//2,sz[1]//2+1,sz[2]//2]+
#                            im[sz[0]//2,sz[1]//2,sz[2]//2-1]+
#                            im[sz[0]//2,sz[1]//2,sz[2]//2+1])/6.0                            
#    return im
         
#def unring2D(im,lp_res=10.0,niters=2):
#    ''' Remove hallo ringing from particle '''
#    mask
#    imnew = im.copy()
#    for it in range(niters):
#        imm = imnew-np.median(imnew)
#        immz = im.copy()
#        immz[imm>0] = 0
#        iml = low_pass_fft2D(immz,lp_res,shape='gaussian')
#        iml = iml*(imm.min()/iml.min())    
#        im  = imm - iml    
#    return im - np.median(im)    
    
#def unring3D(V,lp_res=10.0,niters=10):
#    ''' Remove hallo ringing from particle '''
#    for it in range(niters):
#        Vm=V-np.median(V)
#        Vz = Vm.copy()
#        Vz[Vm>0] = 0
#        Vl = low_pass_fft3D(Vz,lp_res,shape='gaussian')
#        Vl = Vl*(Vm.min()/Vl.min())    
#        V  = Vm - Vl    
#    return V - np.median(V)       

#def removeFPN(im):
#    ''' remove vertical and horizontal lines '''
#    imf = np.fft.fft2(im)
#    imf[0,:] = 0.0
#    imf[:,0] = 0.0
#    return np.float32(np.real(np.fft.ifft2(imf)))
         
#def resize2D(im,scale):
##    dim = np.ndim(im)
#    if np.isclose(scale,1.0):
#        return im
#    if scale < 1.0:
#        #im = low_pass_gausND(im,ANTIALIAS/float(scale),dim)
#        im = low_pass_fft2D(im, scale2lp_res(scale))
#    return zoom(im,scale)      
#    
##%% #### Test background_remove2D_pyramid ##############
#im = np.squeeze(mrc.load('/fhgfs/result/BetaGal_PETG_20141217/drift/BetaGal_PETG_20141217_0409.mrc'))
#lp_res = 1024
#imb = image.background_remove2D_pyramid(im,lp_res)
#pyramid  = tuple(pyramid_gaussian(im, downscale=2))
#pyramidb = tuple(pyramid_gaussian(imb, downscale=2))
#
## find pyramid index that corresponds to low pass lp_res
#pyridx = 3 #int(np.floor(np.log2(lp_res))) 
#imshow(pyramid[pyridx])
#imshow(pyramidb[pyridx])
    
#def low_pass_gaus2D(im, lp_res):
#    ''' Smoothes image to lp_res resolution '''
#    ndim = im.ndim 
#    im   = gaussian_filter1d(im, lp_res/2.0, axis=ndim-1, mode='reflect')
#    return gaussian_filter1d(im, lp_res/2.0, axis=ndim-2, mode='reflect')
   
#def low_pass_gausND(im,lp_res,n):
#    ''' Smoothes image to lp_res resolution using n dimensions'''
#    ndim = im.ndim 
#    for k in range(n):
#        im = gaussian_filter1d(im, lp_res/2.0, axis=ndim-k-1, mode='reflect')
#    return im   
#    
#def low_pass_gaus2D(im, lp_res):
#    return low_pass_gausND(im,lp_res,2)

#def bin3D(im,bn):    
#    im = low_pass_gausND(im,ANTIALIAS*bn,3)
#    #im = gaussian_filter(im, 1.5*bn/2)
#    return decimate3D(im, bn)    
    
#def unbin3D(im,bn):
#    if bn > 1:
#        return zoom(im,bn)
#    else:
#        return im 
    #def tile_meshgrid2D(sz, win_sz, ovlp, **kwargs):
#    xpos, ypos = tile2D(sz, win_sz, ovlp, **kwargs)
#    xpos, ypos = np.meshgrid(xpos, ypos)
#    return xpos, ypos
    
#def radial2D(sz):
#    ''' Creates radius 2D image of size sz ''' 
#    xx,yy = np.meshgrid(np.arange(sz[1], dtype='float32'), np.arange(sz[0], dtype='float32'))
#    return np.sqrt(((np.float32(xx)-sz[1]//2)/sz[1])**2.0 + ((np.float32(yy)-sz[0]//2)/sz[0])**2)    
#        
#def res2freq(sz, res):
#    ''' Converts resolution to frequenncy radius '''
#    return sz/res;    