
# Automatically rescales and resizes an input map to be consistent with a reference.


# Dependencies: Relion and IMOD


# OPTIONAL TODO: add support for references other than .star files listing particle stacks.


import argparse
import subprocess
from   star import star
#from common import *
import numpy as np
from   myutils.utils import sysrun,tprint 
from   myutils import filenames as fn

def allSame(sizes):
	sizes = np.array(sizes,dtype='double')
	return np.all(sizes==sizes[0])

def parse_args():
	parser = argparse.ArgumentParser(
		description='Creates a copy of a given map that is automatically rescaled and resized to the dimensions of particles listed in a reference star file. To be used in conjunction with Relion Class3D and Refine3D.', 
		epilog='Example command: python mrc_autosize.py --i 5a1a_4A.mrc --o 5a1a_4A_bin3.mrc --ref ../../data/betagal_unittest/shiny_particles.star ')
	parser.add_argument('--input', metavar='mrc', type=str, required=True, help="Input mrc to rescale and resize. Required.")
	parser.add_argument('--reference', metavar='mrc', type=str, required=True, help="The input mrc will be altered to match the dimensions of this reference. Must be a .star file listing particle stacks. Required.")
	parser.add_argument('--output', metavar='path', type=str, required=True, help="Save the resized mrc in a file at this location.")

	return parser.parse_args()



# Get the path to the first particle stack .mrcs file listed in a star file, and its listed pixel size.
def getAParticleStack(star_path):
	particle_star = star.starFromPath(star_path)
	line = particle_star.body.next()
	particle_stack = particle_star.valueOf('ImageName', line).split('@')[1]
	particle_angpix = particle_star.valueOf('DetectorPixelSize', line)

	return particle_stack, particle_angpix



# Returns the pixel size and box size of an mrc file, each as a list of dimensions
# e.g. ( [0.425, 0.425, 0.425], [80, 100, 120] ) means the pixel size is 0.425A/px^3, and the box dimensions are 80 x 100 x 120
def getProperties(mrc_path):
	angpix = subprocess.check_output(['header','-pixel', mrc_path]).split()
	
	box_size = subprocess.check_output(['header','-size', mrc_path]).split()

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


def main(input_path, reference_path, out_path):

	# handle input map
	input_angpixs, input_sizes = getProperties(input_path)

	assert allSame(input_angpixs), \
		"Input pixel sizes are not consistent in all dimensions: " + str(input_angpixs)
	
	input_angpix = input_angpixs[0]

	# Output dimensions of input and reference map
	print "Input map " + input_path
	print "Pixel size " + input_angpix + "A/px"
	print "Box size " + input_sizes[0] + " x " + input_sizes[1] + " x " + input_sizes[2]
	print ""





	# handle reference star file
	assert reference_path.endswith('.star'), \
		"The reference path must be a .star file!!"

	reference_mrc_path, ref_angpix = getAParticleStack(reference_path)
	print "Basing calculations on first particle stack found in reference star file."


	_, ref_sizes = getProperties(reference_mrc_path)
	assert ref_sizes[0] == ref_sizes[1], \
		"Reference images must be square. Reference stack dimensions are: " + str(ref_sizes[0]) + " x " + str(ref_sizes[1])
	ref_size = ref_sizes[0]

	print "Reference stack " + reference_mrc_path
	print "Pixel size " + str(ref_angpix) + "A/px"
	print "Box size " + str(ref_size) + "^3"
	print ""



	print "Creating a copy of the input map."
	working_mrc = fn.replace_ext(out_path,'_tmp.mrc') #insertSuffix('_tmp', out_path)
	#shell(['cp', input_path, working_mrc])
	out,err,status = sysrun('cp %s %s' % (input_path,working_mrc))
	assert(status)        



	# if not all the box dimensions of the input mrc are the same, expand the box to the largest dimension
	print "Resizing the map to a cube."
	if not allSame(input_sizes):
		resize(working_mrc, max( int(s) for s in input_sizes))


	print "Rescaling map to pixel size of " + ref_angpix + "A/px"
	rescale(working_mrc, input_angpix, ref_angpix)

	print "Resizing map to box size of " + ref_size + " x " + ref_size + " x " + ref_size
	resize(working_mrc, ref_size)

	# change map name to user-specified destination
	#shell(['mv', working_mrc, out_path])
	out,err,status = sysrun('mv %s %s' % (working_mrc,out_path))
	assert(status)        

	print 'Auto-sizing completed. New map located at: ' + out_path





# Execute if this script is called
if __name__ == '__main__':
	args = parse_args()

	input_path = args.input
	ref_path = args.reference
	out_path = args.output

	main(input_path, ref_path, out_path)