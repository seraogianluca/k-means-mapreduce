import numpy as np, scipy.stats as st
import statsmodels.stats.api as sms
import glob
import os

path = "../tests/spark/100k/output_3_13.txt"

times = []
iteration_time = []
init_centroids = []
iterations = []
with open(path, "r") as file:
    
    for line in file:
        if line.startswith("execution time:"):
            et = round(float(line.split()[2]), 4)
            times.append(et)
        if line.startswith("init centroid execution:"):
            ic = round(float(line.split()[5]), 4)
            init_centroids.append(ic)
        if line.startswith("n_iter"):
            i = int(line.split()[1])
            iterations.append(i)
            iteration_time.append((et-ic)/i)

exec_time = np.array(times)
init_centr = np.array(init_centroids)
iters = np.array(iterations)
it_t = np.array(iteration_time)

with open(path, "a") as file:
    file.write('\n')
    file.write("execution time: {mean:.4f} s \n".format(mean = np.mean(exec_time)))
    lower, upper = sms.DescrStatsW(exec_time).tconfint_mean()
    file.write("execution time confidence interval: [{low:.4f},{up:.4f}] \n".format(low = lower, up = upper))
    file.write("execution time variance: {var:.4f}\n".format(var = np.var(exec_time)))

    file.write("init centroids time: {mean:.4f} s \n".format(mean = np.mean(init_centr)))
    lower, upper = sms.DescrStatsW(init_centr).tconfint_mean()
    file.write("init centroids confidence interval: [{low:.4f},{up:.4f}] \n".format(low = lower, up = upper))
    file.write("init centroids variance: {var:.4f}\n".format(var = np.var(init_centr)))

    file.write("average iterations: {mean:.2f} \n".format(mean = np.mean(iters)))

    file.write("iteration execution time: {mean:.4f} s \n".format(mean = np.mean(it_t)))
    lower, upper = sms.DescrStatsW(it_t).tconfint_mean()
    file.write("iteration execution time confidence interval: [{low:.4f},{up:.4f}] \n".format(low = lower, up = upper))
    file.write("iteration execution time variance: {var:.4f}\n".format(var = np.var(it_t)))
