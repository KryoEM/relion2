# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 14:04:18 2016

@author: worker
"""
import dm4reader
import numpy as np

def read_dm4(fname):
    dm4data = dm4reader.DM4File.open(fname)
    tags    = dm4data.read_directory()
    image_data_tag = tags.named_subdirs['ImageList'].unnamed_subdirs[1].named_subdirs['ImageData']
    image_tag      = image_data_tag.named_tags['Data']
    
    XDim = dm4data.read_tag_data(image_data_tag.named_subdirs['Dimensions'].unnamed_tags[0])
    YDim = dm4data.read_tag_data(image_data_tag.named_subdirs['Dimensions'].unnamed_tags[1])
    np_array = np.array(dm4data.read_tag_data(image_tag), dtype=np.uint16)
    np_array = np.reshape(np_array, (YDim, XDim))
    return np_array