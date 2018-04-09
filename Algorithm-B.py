from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import sys
import time


def convert_vector(file, file_name):
	dimensions = file.readline().split()
	rows = int(dimensions[0])
	cols = int(dimensions[1])
	print(rows)
	print(cols)
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

class MR_matrix_multiplication_TwoStep(MRJob):

	def steps(self):
		return [MRStep(mapper = self.mapper_first_iteration, reducer = self.reducer_first_iteration),
			MRStep(mapper = self.mapper_second_iteration, reducer = self.reducer_second_iteration)]

	def mapper_first_iteration(self, _, line):

		if len(line.split()) == 2:
			return

		i, j, value = line.split()
		filename = os.environ['map_input_file']

		if filename == sys.argv[-2]:
			yield j, ('Matrix-identifier-1', i, value)
		else:
			yield i, ('Matrix-identifier-2', j, value)

	def reducer_first_iteration(self, j, val):

		matrix_1_values  = []
		matrix_2_values  = []

		for values in val:
			if values[0] == 'Matrix-identifier-1':
				matrix_1_values.append(values)
			else:
				matrix_2_values.append(values)

		for (a, i, value_1) in matrix_1_values:
			for (b, k, value_2) in matrix_2_values:
				yield j, (i, k, int(value_1) * int(value_2))

	def mapper_second_iteration(self, j, val):
		yield (val[0], val[1]), val[2]

	def reducer_second_iteration(self, key, val):
		yield key, sum(val)

command_line = sys.argv

matrix_1 = open(command_line[-2], 'r')
matrix_2 = open(command_line[-1], 'r')

convert_vector(matrix_1, command_line[-2])
convert_vector(matrix_2, command_line[-1])

timer_start = time.time()

WORD_RE = re.compile(r"[c\w']+")

MR_matrix_multiplication_TwoStep.run()

timer_end = time.time()
total_time = timer_end - timer_start
print(total_time)
