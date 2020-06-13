# Notes 

## combiner notes

The #1 rule of Combiners are: do not assume that the combiner will run. 
Treat the combiner only as an optimization.

The Combiner is not guaranteed to run over all of your data. 
In some cases when the data doesn't need to be spilled to disk, MapReduce will skip using the Combiner entirely. 
Note also that the Combiner may be ran multiple times over subsets of the data! It'll run once per spill.

In your case, you are making this bad assumption. 
You should be doing the sum in the Combiner AND the Reducer.

The input and output of the combiner needs to be identical (Text,Double -> Text,Double) 
and it needs to match up with the output of the Mapper and the input of the Reducer.


Unlike a Reducer, input/output key and value types of combiner must match the output types of your Mapper .

Combiners can only be used on the functions that are commutative (a.b = b.a) and associative {a.(b.c) = (a.b).c} .
From this, we can say that combiner may operate only on a subset of your keys and values. Or may does not execute at all, 
still, you want the output of the program to remain same.
 
From multiple Mappers, Reducer get its input data as part of the partitioning process. 
Combiners can only get its input from one Mapper.



# Dataset creation 
For the creation of the dataset the Scikit-learn python library was used.
The make_blobs() function generates isotropic Gaussian blobs for clustering.
This was made in order to generate datasets with clustering tendency instead of using a random uniformly distributed set of points, because of that, the number of centers parameter choosed is much higher than the actual number of centroids that will be used to test the app.

