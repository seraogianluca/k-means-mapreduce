from sklearn.datasets import make_blobs
import pandas as pd
import numpy as np
from pandas.plotting._matplotlib import scatter_matrix
from matplotlib import pyplot
from pandas import DataFrame

dimension = 2
samples = 1000
centers = 4

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension)

with open("dataset_2d_4centr.txt", "w") as file:
    for point in points:
        for value in range(dimension):
            if value == (dimension - 1):
                file.write(str(round(point[value], 4)))
            else:
                file.write(str(round(point[value], 4)) + ",")
        file.write("\n")
        
data = np.array(points)

#plot scatterplot
df = pd.DataFrame(data, columns=['x_0','x_1'])
scatter_matrix(df, alpha=0.2, figsize=(10,10))



df = DataFrame(dict(x=points[:,0], y=points[:,1], label=y))
colors = {0:'red', 1:'blue', 2:'green', 3:'black', 4:'purple', 5:'pink', 6:'orange'}
fig, ax = pyplot.subplots()
grouped = df.groupby('label')
for key, group in grouped:
    group.plot(ax=ax, kind='scatter', x='x', y='y', label=key, color=colors[key])
pyplot.show()