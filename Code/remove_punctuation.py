import re
import string
import sys

def remove_punc(file):
	temp_store = open('Matrix_no_punctuation.txt','wb')

	with file:
		for line in file:
			values = re.findall(r"[\w']+",line)
			if len(values) == 2 or len(values) == 1:				
				continue
			temp_store.write(values[0] + ' ' + values[1] + ' ' + values[2]  + '\r\n')
	file.close()
	temp_store.close()


def count_elements(file):
	count = 0;
	with file:
		for line in file:
			row, col, val = line.split()
			if  row != col:
				count = count + int(val)
	print(count)

matrix = open(sys.argv[-1], 'r')
remove_punc(matrix)
matrix = open('Matrix_no_punctuation.txt', 'r')
count_elements(matrix)
