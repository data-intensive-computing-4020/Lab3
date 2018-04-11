from random import *

# Dimensions of the square matrix
N = 2
matrices = ["Matrix1.txt", "Matrix2.txt"]

def create_matrix(matrix_file):
	matrix_file.write(str(N) + ' ' + str(N) + '\n')
	for x in range(0, N):
		for y in range(0, N):
			value = randint(1, 10)
			matrix_file.write(str(x) + ' ' + str(y) + ' ' + str(value)+ '\n')




matrix_file_1 = open(matrices[0], 'wb')
create_matrix(matrix_file_1)
matrix_file_2 = open(matrices[1], 'wb')
create_matrix(matrix_file_2)



