from sklearn.cluster import KMeans
import numpy as np

points = []
with open("dataset.txt", "r") as file:
    
    for line in file:
        comps = line.split(",")
        point = [float(comps[i]) for i in range (len(comps)) ] 
        points.append(point)
        
dataset = np.array(points)
kmeans = KMeans(n_clusters=2, max_iter=40, random_state=0).fit(dataset)

print(dataset)
print(kmeans.cluster_centers_)
print(kmeans.n_iter_)
