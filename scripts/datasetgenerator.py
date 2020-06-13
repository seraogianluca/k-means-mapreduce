from sklearn.datasets import make_blobs
from matplotlib import pyplot
from pandas import DataFrame

dimension = 3
samples = 1000
centers = 40

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension)

with open("dataset[1k,3].txt", "w") as file:
    for point in points:
        for value in range(dimension):
            if value == (dimension - 1):
                file.write(str(point[value]))
            else:
                file.write(str(point[value]) + ",")
        file.write("\n")

df = DataFrame(dict(x=points[:,0], y=points[:,1], label=y))
fig, ax = pyplot.subplots()
grouped = df.groupby('label')

for key, group in grouped:
    group.plot(ax=ax, kind='scatter', x='x', y='y')
pyplot.show()

"""
fig, axs = pyplot.subplots(2,2)
axs[0, 0].scatter(X[0], X[1])
axs[1, 0].scatter(X[2], X[3])
axs[0, 1].scatter(X[4], X[5])
axs[1, 1].scatter(X[5], X[6])

pyplot.show()
"""

