from scipy.spatial import distance

#[(1, 2, 3.3, -4, 5.6, 6.88, 7)]

# XA = [(1, 2, 3, -4, 5, 6, 7)]
# XB = [(2, 1, 0, 1, 2, 3, 4)]

XA = [(1, 2)]
XB = [(2, 1)]


eu = distance.cdist(XA, XB, 'euclidean')

mink = distance.cdist(XA, XB, 'minkowski', p=5.)

manat = distance.cdist(XA, XB, 'cityblock') #manhattan

cheb = distance.cdist(XA, XB, 'chebyshev')

print("Euclidean:" + str(eu.item(0)))
print("Minkowski:" + str(mink.item(0)))
print("Manhattan:" + str(manat.item(0)))
print("Chebishev:" + str(cheb.item(0)))