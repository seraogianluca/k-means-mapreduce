from sklearn.datasets import make_blobs

dimension = 3
samples = 1000
centers = 40

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension)

with open("dataset.txt", "w") as file:
    for point in points:
        for value in range(dimension):
            if value == (dimension - 1):
                file.write(str(round(point[value], 4)))
            else:
                file.write(str(round(point[value], 4)) + ",")
        file.write("\n")

