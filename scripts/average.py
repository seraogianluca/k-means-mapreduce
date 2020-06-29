from statistics import mean

path = "../benchmarks/100k/output_7_7.txt"

with open(path, "r") as file:
    times = []
    iterations = []
    for line in file:
        if line.startswith("execution time"):
            times.append(float(line.split()[2]))
        if line.startswith("n_iter"):
            iterations.append(int(line.split()[1]))


with open(path, "a") as file:
    file.write('\n')
    file.write("average time: {mean: .4f} ms \n".format(mean = mean(times)))
    file.write("average iterations: {mean: .4f} \n".format(mean = mean(iterations)))