from mrjob.job import MRJob
import re
import os
import sys
import time


def convert_sparse(file, file_name):

	temp_store = open('output.txt','wb')
	dimensions = file.readline().split()
	rows = int(dimensions[0])
	cols = int(dimensions[1])
	temp_store.write(str(rows) + ' ' + str(cols) + '\r\n')
	row_number = 0
	col_number = 0

	with file:
		for line in file:
			value = line.split()

			x = int(value[0])
			y = int(value[1])

			if col_number < cols and x > row_number:

				while col_number < cols :
					temp_store.write(str(row_number) + ' ' + str(col_number) + ' 0\r\n')
					col_number += 1

			if int(value[1]) == col_number:
				temp_store.write(value[0] + ' ' + value[1] + ' ' + value[2] + '\r\n')
				col_number += 1
				
			elif int(value[1]) > col_number:
				while col_number < y :
					temp_store.write(str(x) + ' ' + str(col_number) +  ' 0\r\n')
					col_number += 1

				temp_store.write(value[0] + ' ' + value[1] + ' ' + value[2] + '\r\n')
				col_number += 1

			if col_number == cols:
				col_number = 0
				row_number += 1
	
	file.close()
	temp_store.close()
	
	with open("output.txt") as file_from:
		with open(file_name, "w") as file_to:
				for line in file_from:
					file_to.write(line)


class MRMatrixReduceOneStep(MRJob):
	
    def mapper(self, _, line):

		if len(line.split()) == 2:
			return 

		x, y, value = line.split()
		filename = os.environ['map_input_file']

		if filename == sys.argv[-2]:
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


time_start = time.time()

WORD_RE = re.compile(r"[\w']+")

command_line = sys.argv

matrix_1 = open(command_line[-2], 'r')
matrix_2 = open(command_line[-1], 'r')

convert_sparse(matrix_1,command_line[-2])
convert_sparse(matrix_2,command_line[-1])

matrix_1 = open(command_line[-2], 'r')
matrix_2 = open(command_line[-1], 'r')

i, j1 = matrix_1.readline().split()
j2, k = matrix_2.readline().split()

checked_dimensions = 0

print('Matrx 1 dimensions: i -> '+ i + ' & j -> ' + j1)
print('Matrx 2 dimensions: j -> '+ j2 + ' & k -> ' + k)

if j1 != j2:
	print('The matrices do not have the correct dimensions to be multiplied')
	sys.exit("Error message")

j = j1

matrix_1.close()
matrix_2.close()

MRMatrixReduceOneStep.run()

time_end = time.time()
total_time = time_end - time_start
print(total_time)
