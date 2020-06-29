from statistics import mean

with open("output_3_7.txt", "r") as file:
    times = []
    iterations = []
    for line in file:
        if line.startswith("execution time"):
            times.append(float(line.split()[2]))
        if line.startswith("n_iter"):
            iterations.append(int(line.split()[1]))


with open("output_3_7.txt", "a") as file:
    file.write('\n')
    file.write("average time: {mean: .4f} ms \n".format(mean = mean(times)))
    file.write("average iterations: {mean: .4f} \n".format(mean = mean(iterations)))