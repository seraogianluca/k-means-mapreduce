import numpy as np, scipy.stats as st
import statsmodels.stats.api as sms
import glob
import os

path = "../tests/hadoop/no_opt_output_3_7_2.txt"

times = []
init_centroids = []
iterations = []
with open(path, "r") as file:
    
    for line in file:
        if line.startswith("execution time:"):
            times.append(round(float(line.split()[2])/1000, 4))
        if line.startswith("init centroid execution:"):
            init_centroids.append(round(float(line.split()[3])/1000, 4))
        if line.startswith("n_iter"):
            iterations.append(int(line.split()[1]))

exec_time = np.array(times)
init_centr = np.array(init_centroids)
iters = np.array(iterations)

with open(path, "a") as file:
    file.write('\n')
    file.write("execution time: {mean:.4f} s \n".format(mean = np.mean(exec_time)))
    lower, upper = sms.DescrStatsW(exec_time).tconfint_mean()
    file.write("execution time confidence interval: [{low:.4f},{up:.4f}] \n".format(low = lower, up = upper))
    file.write("execution time variance: {var:.4f}\n".format(var = np.var(exec_time)))

    file.write("init centroids time: {mean:.4f} ms \n".format(mean = np.mean(init_centr)))
    lower, upper = sms.DescrStatsW(init_centr).tconfint_mean()
    file.write("init centroids confidence interval: [{low:.4f},{up:.4f}] \n".format(low = lower, up = upper))
    file.write("init centroids variance: {var:.4f}\n".format(var = np.var(init_centr)))

    file.write("average iterations: {mean:.2f} \n".format(mean = np.mean(iters)))