class Point:
    
    def __init__(self, a):
        self.components = a
        self.dimension = len(a)
        self.number_of_points = 1
    
    def sum(self, p):
        for i in range(0, self.dimension):
            self.components[i] = self.components[i] + p.components[i]
        self.number_of_points += p.number_of_points

    def distance(self, p, h):
        if (h < 0):
           h = 2
        if (h == 0):
            # Chebyshev distance
            max = -1.0
            diff = 0.0
            
            for i in range(0, self.dimension):
                diff = abs(self.components[i] - p.components[i])
                if (diff > max):
                    max = diff
            return max
        else:
            dist = 0.0
            for i in range(0, self.dimension):
                dist += abs(self.components[i] - p.components[i]) ** h
            dist = dist ** (1.0/h)
            return dist

    def get_average_point(self):
        average_point = Point(self.components)
        for i in range(0, self.dimension):
            average_point.components[i] /= self.number_of_points
        return average_point
    
    def __str__(self):
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result

    def __repr__(self):
        # Spark uses this method when save on text file
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result