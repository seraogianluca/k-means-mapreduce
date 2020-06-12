import numpy as np
import sklearn 


dim = 7
numpoint = 10

np.random.seed(42)
while(numpoint>0):
    f = open("dataset", "a+") 
    mat = np.random.rand(1,dim)
    print(mat)

    for i in range(dim): 

        f.write(str(10*(mat[0][i])))

        if i != dim-1 :
            f.write(",")

    f.write("\n")
    f.close()
    numpoint -= 1