from sklearn.datasets import make_blobs
from matplotlib import pyplot
from pandas import DataFrame

dimension = 3
samples = 1000
centers = 40

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension)

with open("dataset.txt", "w") as file:
    for point in points:
        for value in range(dimension):
            if value == (dimension - 1):
                file.write(str(point[value]))
            else:
                file.write(str(point[value]) + ",")
        file.write("\n")

