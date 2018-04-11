# ELEN4020 - Data Intensive Computing
## Lab 3 - Matrix Multiplication using MapReduce
The lab report pdf and latex file can be found in the Report Folder. Furthermore the source code that was written and used for the lab can be found in the Code folder.

This lab involved the implementation of 2 algorithms that would achieved matrix multiplication using MapReduce. These algorithms are then tested on square matrices multiplied by themselves. The 2 algorithms are as follows:

1. Naive Iteration One step - This algorithm does matrix multiplication on using one mapper and one reduce function.
2. Naive Iteration Two step - This algorithm does matrix multiplication on using two mapper and two reduce functions.

Included are 2 more algorithms: MatrixGenerator.py to create square matrices of N x N dimensions and remove_punctuation.py which removes the punctuation from the input textfile and counts sums the elements. The remove_punctuation.py is used to count the number of paths for the graph.

### Installation instructions

Prerequisites:

Make sure [MrJob](https://github.com/Yelp/mrjob) is installed.

In a terminal:

```bash

# Clone the Repo
git clone https://github.com/data-intensive-computing-4020/Lab3.git

# Change to the correct directory
cd Lab3

# To generate an N by N matrix
python MatrixGenerator.py

# To run Algorithm-A or Algorithm-B
python <algorithm> <input_matrix_A> <input_matrix_B>

# Example
python Algorithm-A.py outA1.list outB1.list

# Process for graph matrix (outNetwork.list)
python <algorithm> outNetwork1.list outNetwork2.list > output.txt
python remove_punctuation.py output.txt

# This will generate Matrix_no_punctuation.txt
python <algorithm> outNetwork1.list Matrix_no_punctuation.txt > output_final.txt
python remove_punctuation.py output_final.txt


```
