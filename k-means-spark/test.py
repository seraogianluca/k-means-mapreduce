from point import Point
from termcolor import colored

# 2-dimension test
print(colored('\n2-dimensional points\n', 'red'))
p = Point([1, 2])
q = Point([2, 1])

print(colored("Distance Test\n", "blue"))

print("2-dimension-man ****",abs(p.distance(q, 1) - 2.0) < 0.0001)
print("2-dimension-euc ****",abs(p.distance(q, 2) - 1.4142) < 0.0001)
print("2-dimension-min ****",abs(p.distance(q, 5) - 1.1486) < 0.0001)
print("2-dimension-inf ****",abs(p.distance(q, float("inf")) - 1.0) < 0.0001)
    
# 3-dimension test
print(colored('\n5-dimensional points\n', 'red'))

p = Point([1, 2, 3])
q = Point([2, 1, 0])

print(colored("Distance Test\n", "blue"))

print("3-dimension-man ****",abs(p.distance(q, 1) - 5) < 0.0001)
print("3-dimension-euc ****",abs(p.distance(q, 2) - 3.3166) < 0.0001)
print("3-dimension-min ****",abs(p.distance(q, 5) - 3.0049) < 0.0001)
print("3-dimension-inf ****",abs(p.distance(q, float("inf")) - 3.0) < 0.0001)

# 7-dimension test
print(colored('\n7-dimensional points\n', 'red'))

p = Point([1, 2, 3, -4, 5, 6, 7])
q = Point([2, 1, 0, 1, 2, 3, 4])

print(colored("Distance Test\n", "blue"))

print("7-dimension-man ****",abs(p.distance(q, 1) - 19.0) < 0.0001)
print("7-dimension-euc ****",abs(p.distance(q, 2) - 7.9372) < 0.0001)
print("7-dimension-min ****",abs(p.distance(q, 5) - 5.2788) < 0.0001)
print("7-dimension-inf ****",abs(p.distance(q, float("inf")) - 5.0) < 0.0001)

print("")