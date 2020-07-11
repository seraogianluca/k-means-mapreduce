from sklearn.cluster import KMeans
import numpy as np
import time
from statistics import mean
import pandas as pd


points = []
clusters= 4

start_milli_time = round(time.time() * 1000, 4)
with open("dataset_2d_4centr.txt", "r") as file:
    for line in file:
        comps = line.split(",")
        point = [float(comps[i]) for i in range (len(comps)) ] 
        points.append(point)

dataset = np.array(points)
kmeans = KMeans(n_clusters=clusters, init='random', precompute_distances=False, random_state=0).fit(dataset)

end_milli_time = round(time.time() * 1000, 4)
execution_time = round(end_milli_time - start_milli_time, 4)

#with open('output_2d.txt', 'a') as f:
#    f.write("execution time: " + str(execution_time) + ' ms \n')
#    f.write('centroids:\n'+ str(kmeans.cluster_centers_) + '\n')
#    f.write('n_iter: ' + str(kmeans.n_iter_) + '\n\n')



print(str(execution_time))
print(str(kmeans.cluster_centers_))
print(str(kmeans.n_iter_))



#plot black points and red centroids
cent = kmeans.cluster_centers_
centr = np.array(cent) 

#concatena i punti con i centroidi
c = np.vstack([dataset, centr])

df = pd.DataFrame({'x': c[:, 0], 'y': c[:, 1]})


# Declare a list that is to be converted into a column 
labels = []
for i in range(0,1000):
    labels.append('black')
for i in range(0,4):
    labels.append('red')

df['label'] = labels

print(df)

df.plot(x='x', y='y', c=df['label'], kind='scatter')