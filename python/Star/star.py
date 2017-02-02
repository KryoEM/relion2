import os
import sys
from common import *



#===============================================================================================================
# HANDLING STAR FILES
#===============================================================================================================
# returns if the file at path 'path' is likely to be a star file or not by looking for a parameter definition (e.g. "_rlnMicrographName #2")
def isStar(path):
	with open(path) as file:
		for line in file:
			if isParameterDef(line):
				return True
		# reached end of file without finding a parameter definition
		return False


# returns if the file at path 'path' is likely to be a star file containing particles or not
# criterion: if parameter 'ImageName' exists
def isParFile(path):
	if isStar(path):
		star_file = starFromPath(path)
		return star_file.paramExists('ImageName')

# returns if the file at path 'path' is likely to be a star file containing only coordinate information
# criterion: has 'CoordinateX' and 'CoordinateY' parameters, but NOT 'MicrographName' parameter 
def isCoordFile(path):
	if isStar(path):
		star_file = starFromPath(path)
		return not star_file.paramExists('MicrographName') and star_file.paramExists('CoordinateX') and star_file.paramExists('CoordinateY')




# Returns if a line is a parameter definition line e.g. "_rlnMicrographName #2"
def isParameterDef(line):
	return firstWord(line).startswith("_rln")

# Returns if a line could be part of the header
def isHeader(line):
	first_word = firstWord(line)

	return first_word == "" \
		or first_word.startswith("data_") \
		or first_word.startswith("loop_") \
		or isParameterDef(line)




#===============================================================================================================
# STAR OBJECTS
#===============================================================================================================

# A Star object contains all the information found in a star file.

# It consists of two parts:
# (1) a lookup table converting parameter names to their column number. This is generally derived from a .star file's header.
# 		e.g. lookup = { 'MicrographName' : 1, 'CoordX' : 2, 'CoordY' : 3 }

# (2) a generator that yields each element of the star's body, as a dictionary of parameter name to value
# 		e.g. element = { 'MicrographName' : 'Mic001.mrc', 'CoordX' : 3845.2, 'CoordY' : 2384.3 }
#			 body 	 = ( e1, e2, e3 )

# The easiest way to create a Star object from a .star file is to use the function starFromPath(path)


class Star:

	# A Star object is initialized by being given a parameter lookup table, and a body generator
	# Therefore, there does not need to be an existing .star file on the filesystem in order to create a Star object (useful when you want to create a copy of a Star object with modified traits).
	def __init__(self, lookup, body):
		self.lookup = lookup
		self.body = body


	# Comparators
	# Two Star objects are equal when they have equal lookup tables and equal contents in their generators
	def __eq__(self, other):
		return self.lookup == other.lookup and list(self.body) == list(other.body)
	# Two Star objects are not equal when they do not satisfy the equality requirements
	def __ne__(self,other):
		return not(self.__eq__)



	# Return whether or not the parameter param exists in the star file
	def paramExists(self,param):
		return (param in self.lookup)


	# Return the column number of a given parameter (count begins at 0)
	def numOf(self,param):
		return self.lookup[param]

	# Return the column numbers of a list of parameters (count begins at 0)
	def numsOf(self, params):
		return( map (lambda param: self.numOf(param), params) )
			



	# Returns the entire body of elements in the star file as a list
	# useful if you need to use functions available only to lists, and not generators
	def list(self):
		return list(self.body)



	# Returns a list of all values in the Star file of parameter 'param'
	# e.g. listAll('MicrographName') will return a list of all micrograh names
	def listAll(self, param):
		return map( lambda e: e[param], self.body)






#===============================================================================================================
# READING IN STAR FILES
#===============================================================================================================


# Returns a lookup table for parameter names and their corresponding column number in a given star file header
# key:value pair is parameter_name : column_number
# e.g. { 'MicrographName' : 1, 'CoordX' : 2, 'CoordY' : 3 }
def headerLookup(star_path):	
	with open(star_path) as file_object:

		lookup = {}

		for line in file_object:

			if isParameterDef(line):
				# populate the lookup table
				words = line.split()
				param_name = stripPrefix(words[0], '_rln')
				column = int(words[1].strip('#')) - 1
				lookup[param_name] = column
				# TO DO
				# warning if param already exists

			#stop searching if we are no longer on a header line, and the header list is populated
			elif not isHeader(line) and lookup != {}:
				break

	return lookup


# Returns a generator for which each element is a dictionary object describing certain values and parameters for a given line in a .star file
# e.g. element = { 'MicrographName' : 'Mic001.mrc', 'CoordX' : 3845.2, 'CoordY' : 2384.3 }
#	   body    = ( e1, e2, e3 )
def bodyGenerator(lookup, star_path):
	with open(star_path) as file_object:
		for line in file_object:
			# skip any header lines
			if isHeader(line):
				continue
			else:
				element = {}
				values = line.split()

				for param, col in lookup.items():
					try:
						element[param] = values[col]
					except KeyError:
						print >> sys.stderr, "WARNING: Could not find parameter " + str(param) + " in the header of the given star file."
						element[param] = None
					except IndexError:
						# if the list of values is not empty and we have an index error, the line ended before the appropriate column number.
						if values:
							print >> sys.stderr, 'WARNING: value ' + param + ' missing from the following line: '
							print >> sys.stderr, line
						# if the list of values is empty, the line was probably empty.
						else:
							print >> sys.stderr, 'WARNING: Empty lines found in body of star file. If problems arise, please try deleting the empty lines.'
						element[param] = None

				yield element	


# Create a Star object from a path to a .star file
def starFromPath(star_path):
	lookup = headerLookup(star_path)
	return Star(lookup, bodyGenerator(lookup, star_path))


# Get a single value from a star element
def getValue(element, param):
	return element[param]

def getMicName(element):
	return element['MicrographName']

# Get multiple values from the same element
def getValues(element, params):
	return map (lambda param: valueOf(element, param), params)











#===============================================================================================================
# WRITING OUT STAR FILES
#===============================================================================================================

# Sorted list of parameters in a parameter lookup table, in order of column number
# especially useful to feed into makeTabbedLine()
def orderedParams(lookup):
	return [ param for param in sorted(self.lookup, key=self.lookup.get) ]


# Returns a star file header as a printtable string, given a lookup table
# e.g. textHeader({ 'MicrographName' : 1 }) will return "\ndata_\n\nloop_\n_rlnMicrographName #1\n"
def textHeader(lookup):
	parameters = orderedParams(lookup)

	header = "\ndata_\n\nloop_\n"
	for index, parameter in enumerate(parameters, start=1):
		header += "_rln" + parameter + " #" + str(index) + "\n"

	return header


# Given a star element and a list of parameters, write out a tab-delimited line of the element's values for those parameters, in the order given.
def makeTabbedLine(element, params):
	line = ""
	for param in params:
		line += str(element[param]) + "\t"
	line = line.rstrip("\t") + "\n"

	return line
