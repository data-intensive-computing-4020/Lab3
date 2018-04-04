from mrjob.job import MRJob
import re
import os
import sys


class MRMatrixReduceOneStep(MRJob):

    def mapper(self, _, line):

		if len(line.split()) == 2:
			return 

		x, y, value = line.split()
		filename = os.environ['map_input_file']

		if filename == 'Matrix1.txt':
			for col in range(0, int(k)):
				yield (int(x), col), ('Matrix-identifier-1', int(y), int(value))
		else:
			for row in range(0, int(i)):
				yield (row, int(y)), ('Matrix-identifier-2', int(x), int(value))

    def reducer(self, key, val):

		matrix_1_values  = []
		matrix_2_values  = []
		answer_matrix    = []

		for values in val:
			if values[0] == 'Matrix-identifier-1':
				matrix_1_values.append(values[2])
			else:
				matrix_2_values.append(values[2])

		for iterator in range(0, int(j)):
			answer_matrix.append(matrix_1_values[iterator] * matrix_2_values[iterator])
		yield key, sum(answer_matrix)


WORD_RE = re.compile(r"[\w']+")

matrix_1 = open('Matrix1.txt', 'r')
matrix_2 = open('Matrix2.txt', 'r')

i, j1 = matrix_1.readline().split()
j2, k = matrix_2.readline().split()

print('Matrx 1 dimensions: i -> '+ i + ' & j -> ' + j1)
print('Matrx 2 dimensions: j -> '+ j2 + ' & k -> ' + k)

if j1 != j2:
	print('The matrices do not have the correct dimensions to be multiplied')
	sys.exit("Error message")

j = j1

matrix_1.close()
matrix_2.close()

MRMatrixReduceOneStep.run()
