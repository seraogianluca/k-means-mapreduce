from scipy.spatial import distance

XA = [(35.0456, -85.2672),
       (35.1174, -89.9711),
       (35.9728, -83.9422),
       (36.1667, -86.7833)]

XB = [(35.0456, -85.2672),
       (35.1174, -89.9711),
       (35.9728, -83.9422),
       (36.1667, -86.7833)]


Y = cdist(XA, XB, 'euclidean')

#Y = cdist(XA, XB, 'minkowski', p=2.)

#Y = cdist(XA, XB, 'cityblock') #manhattan

#Y = cdist(XA, XB, 'chebyshev')

print(Y)