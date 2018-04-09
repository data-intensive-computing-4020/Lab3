from mrjob.job import MRJob
import re
import os
import sys
import time					


def convert_vector(file, file_name):

	dimensions = file.readline().split()
	rows = int(dimensions[0])
	cols = int(dimensions[1])
	if rows != 1 and cols != 1:
		return
	
	temp_store = open('temp_output.txt','wb')
	temp_store.write(str(rows) + ' ' + str(cols) + '\r\n')

	with file:
		for line in file:
			value = line.split()

			x = value[0]
			magnitude = value[1]
			
			if cols == 1:
				temp_store.write(x + ' 0 ' + magnitude + '\r\n')
			else:
				temp_store.write('0 ' + x + magnitude + '\r\n')
	
	file.close()
	temp_store.close()
	
	with open("temp_output.txt") as file_from:
		with open(file_name, "w") as file_to:
				for line in file_from:
					file_to.write(line)
	
	file.close()
	temp_store.close()

class MR_matrix_multiplication_OneStep(MRJob):
	
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
    
		answer_matrix    = []

		matrix_1_values = [0] * int(j)
		matrix_2_values = [0] * int(j)

		for values in val:
			if values[0] == 'Matrix-identifier-1':
				position = values[1]
				matrix_1_values[position] = values[2]
			else:
				position = values[1]
				matrix_2_values[position] = values[2]
		
		for iterator in range(0, int(j)):
			answer_matrix.append(matrix_1_values[iterator] * matrix_2_values[iterator])
		yield key, sum(answer_matrix)

command_line = sys.argv

matrix_1 = open(command_line[-2], 'r')
matrix_2 = open(command_line[-1], 'r')

convert_vector(matrix_1, command_line[-2])
convert_vector(matrix_2, command_line[-1])

time_start = time.time()

WORD_RE = re.compile(r"[\w']+")

matrix_1 = open(command_line[-2], 'r')
matrix_2 = open(command_line[-1], 'r')

i, j1 = matrix_1.readline().split()
j2, k = matrix_2.readline().split()

checked_dimensions = 0

print('Matrix 1 dimensions: i -> '+ i + ' & j -> ' + j1)
print('Matrix 2 dimensions: j -> '+ j2 + ' & k -> ' + k)

if j1 != j2:
	print('The matrices do not have the correct dimensions to be multiplied')
	sys.exit("Error message")

j = j1

matrix_1.close()
matrix_2.close()

MR_matrix_multiplication_OneStep.run()

time_end = time.time()
total_time = time_end - time_start
print(total_time)
