# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 18:13:58 2014

@author: worker
"""

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
#import myutils.image as image
import matplotlib.cm as cm
from   functools import partial

#from pylab import *

def hist(x,bins):
    plt.hist(x, bins=bins)
    show()

def show():
    plt.ion()
    plt.show()
    plt.draw()

def __format_coord(x,y,im,numcols,numrows):
    col = int(x+0.5)
    row = int(y+0.5)
    if col>=0 and col<numcols and row>=0 and row<numrows:
        val = im[row,col]
        return 'x=%1.4f, y=%1.4f, val=%1.4f'%(x, y, val)
    else:
        return 'x=%1.4f, y=%1.4f'%(x, y)                   

def imshow(im):
    fig = plt.figure()    
    ax  = fig.gca() #add_subplot(111)
#    #im  = image.histeq(im)
    ax.imshow(np.squeeze(im), cmap=plt.cm.gray, interpolation='nearest') 
              #vmin=0.5*im.min(), vmax=2*im.max())
    numrows, numcols = im.shape  
    ax.format_coord = partial(__format_coord,im=im,numrows=numrows,numcols=numcols)  
    plt.show()
    #show()
    
def show_planes(v):
    sz  = v.shape
    fig = plt.figure()    
    ax0 = fig.add_subplot(131)
    im0 = v[:,:,sz[0]//2]
    im1 = v[:,sz[1]//2,:]
    im2 = v[sz[2]//2]
    ax0.imshow(im0,cmap=plt.cm.gray, interpolation='nearest')
    ax1 = fig.add_subplot(132)
    ax1.imshow(im1,cmap=plt.cm.gray, interpolation='nearest')
    ax2 = fig.add_subplot(133)
    ax2.imshow(im2,cmap=plt.cm.gray, interpolation='nearest')
    fp0 = partial(__format_coord,im=im0,numrows=sz[1],numcols=sz[2])
    fp1 = partial(__format_coord,im=im1,numrows=sz[0],numcols=sz[2])
    fp2 = partial(__format_coord,im=im2,numrows=sz[0],numcols=sz[1])
    ax0.format_coord = fp0     
    ax1.format_coord = fp1     
    ax2.format_coord = fp2      
    fig.subplots_adjust(wspace=.1,hspace=0.2,
                        left=0.02,right=0.98,
                        bottom=0.05,top=0.93)    
    show()
    
def quiver(x,y,lenx,leny, **kwargs):
    im  = kwargs.get('image')
    fig = kwargs.get('fig')
    if fig is None: fig = plt.figure()
    if not im is None: plt.imshow(im, cmap=plt.cm.gray, interpolation='nearest')    
    
    fig.gca().quiver(y,x,leny,lenx,pivot='mid',color='b',scale_units='xy', angles='xy', scale=1)
    show()
    
def plot(*args, **kwargs):  
    fig = plt.figure()
    ax  = fig.gca()
    ax.plot(*args, **kwargs)
    ax.plot(*args, **kwargs)
    #plt.plot(*args, **kwargs)
    show()
#%%
    
def plot3D(points,**kwargs):
    #color = kwargs.pop('color','b')
    #fig   = kwargs.pop('fig',None)
    #if fig is None: 
    fig = plt.figure()
    plt.ion()
    ax  = fig.add_subplot(111, projection='3d', aspect='equal')
    if points.ndim == 3:
        n_groups = points.shape[0]
    else:
        assert(points.ndim == 2)
        n_groups = 1;
        points   = points[None,:,:]
        
    colors = cm.rainbow(np.linspace(0, 1, n_groups+1))        
    for g in range(n_groups):
        ax.scatter(points[g,:,0],points[g,:,1],points[g,:,2],c=colors[g]) 
        #ax.scatter(cp[g,0,0],cp[g,0,1],cp[g,0,2],c=colors[-1]) 
        
    #ax.set_aspect('equal')
    plt.show()
    
def clf():
    plt.close('all')

            
