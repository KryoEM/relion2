#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 16:58:01 2016

@author: Oleg Kuybeda
"""

import subprocess
import numpy as np
import argparse
from   myutils.utils import sysrun,tprint


def allSame(sizes):
	sizes = np.array(sizes,dtype='double')
	return np.all(sizes==sizes[0])

def getProperties(mrc_path):
    '''
    :param mrc_path: Returns the pixel size and box size of an mrc file, each as a list of dimensions
    :return: e.g. ( [0.425, 0.425, 0.425], [80, 100, 120] ) means the pixel size is 0.425A/px^3, and the box dimensions are 80 x 100 x 120
    '''
    angpix   = subprocess.check_output(['header', '-pixel', mrc_path]).split()
    box_size = subprocess.check_output(['header', '-size', mrc_path]).split()
    return angpix, box_size

def rescale(mrc_path, pix, new_pix, out=None):
	cmd = ['relion_image_handler', '--i', mrc_path, '--angpix', str(pix), '--rescale_angpix', str(new_pix)]
	if out:
		cmd.append('--o', out)

	print subprocess.check_output(cmd)

def resize(mrc_path, size, out=None):
	cmd = ['relion_image_handler', '--i', mrc_path, '--new_box', str(size)]
	if out:
		cmd.append('--o', out)

	print subprocess.check_output(cmd)

def main(input_path,out_path,opsize,obox):
    # handle input map
    ipsize, ibox = getProperties(input_path)

    assert allSame(ipsize), "Input pixel sizes are not consistent in all dimensions: " + str(ipsize)
    ipsize = ipsize[0]

    # Output dimensions of input and reference map
    #print "Input map " + input_path
    #print "Pixel size " + input_angpix + "A/px"
    #print "Box size " + input_sizes[0] + " x " + input_sizes[1] + " x " + input_sizes[2]
    #print ""

    # handle reference star file
    #assert reference_path.endswith('.star'), \
    #    "The reference path must be a .star file!!"

    #reference_mrc_path, ref_angpix = getAParticleStack(reference_path)
    #print "Basing calculations on first particle stack found in reference star file."

    #_, ref_sizes = getProperties(reference_mrc_path)
    #assert ref_sizes[0] == ref_sizes[1], \
    #    "Reference images must be square. Reference stack dimensions are: " + str(ref_sizes[0]) + " x " + str(
    #        ref_sizes[1])
    #ref_size = ref_sizes[0]

    #print "Reference stack " + reference_mrc_path
    #print "Pixel size " + str(ref_angpix) + "A/px"
    #print "Box size " + str(ref_size) + "^3"
    #print ""

    #print "Creating a copy of the input map."
    #working_mrc = fn.replace_ext(out_path, '_tmp.mrc')  # insertSuffix('_tmp', out_path)
    # shell(['cp', input_path, working_mrc])
    out, err, status = sysrun('cp %s %s' % (input_path, out_path))
    assert (status)

    # if not all the box dimensions of the input mrc are the same, expand the box to the largest dimension
    print "Resizing the map to a cube."
    if not allSame(ibox):
        resize(out_path, max(int(s) for s in ibox))

    print "Rescaling map to pixel size of %.4f" % opsize + " A/px"
    rescale(out_path, ipsize, opsize)

    print "Resizing map to box size of %d" % obox
    resize(out_path, obox)

    # change map name to user-specified destination
    # shell(['mv', working_mrc, out_path])
    #out, err, status = sysrun('mv %s %s' % (working_mrc, out_path))
    #assert (status)

    #print 'Auto-sizing completed. New map located at: ' + out_path

def parse_args():
	parser = argparse.ArgumentParser(
		description='Creates a copy of a given map that is automatically rescaled and resized to the dimensions of particles listed in a reference star file. To be used in conjunction with Relion Class3D and Refine3D.',
		epilog='Example command: python relion_mrc_resize.py -i 5a1a_4A.mrc -o 5a1a_4A_bin3.mrc --opsize 0.33 --obox 378')
	parser.add_argument('-i','--input', type=str, required=True, help="Input mrc to rescale and resize. Required.")
	parser.add_argument('-o','--output', type=str, required=True, help="The output filename.")
	parser.add_argument('-opsize', '--output_pixel_size', type=float, required=True, help="Pixel size to convert to.")
	parser.add_argument('-obox', '--output_box_size', type=int, required=True, help="Box size to convert to.")
	return parser.parse_args()


# Execute if this script is called
if __name__ == '__main__':
	args = parse_args()

	main(args.input,args.output,args.output_pixel_size,args.output_box_size)