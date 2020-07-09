import numpy as np
from numpy import linalg

class Point:
    
    def __init__(self, line):
        values = line.split(",")
        self.components = np.array([round(float(k), 5) for k in values])
        self.number_of_points = 1

    def sum(self, p):
        self.components = np.add(self.components, p.components)
        self.number_of_points += p.number_of_points
        return self

    def distance(self, p, h):
        if (h < 0):
           h = 2
        return round(linalg.norm(self.components - p.components, h), 5)

    def get_average_point(self):
        self.components = np.around(np.divide(self.components, self.number_of_points), 5)
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