class Point:
    
    def __init__(self, line):
        self.components = [round(float(k), 5) for k in line]
        self.dimension = len(self.components)
        self.number_of_points = 1

    def sum(self, p):
        for i in range(self.dimension):
            self.components[i] = self.components[i] + p.components[i]
        self.number_of_points += p.number_of_points
        return self

    def distance(self, p, h):
        if (h < 0):
           h = 2
        if (h == 0):
            # Chebyshev distance
            diff = []
            for i in range(0, self.dimension):
                diff.append(abs(self.components[i] - p.components[i]))
            return max(diff)
        else:
            dist = 0.0
            for i in range(self.dimension):
                dist += abs(self.components[i] - p.components[i]) ** h
            dist = dist ** (1.0/h)
            return dist

    def get_average_point(self):
        self.components = [round(comp/self.number_of_points, 5) for comp in self.components]
        return self
    
    def __str__(self):
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result.strip()

    def __repr__(self):
        # Spark uses this method when save on text file
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result.strip()