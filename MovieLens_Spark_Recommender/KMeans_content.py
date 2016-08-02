from pyspark import SparkConf, SparkContext
from pyspark.mllib.util import MLUtils
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
import numpy as np
import sys
import matplotlib.pyplot as plt

#/Users/jamesledoux/spark-1.6.1/bin/spark-submit /Users/jamesledoux/Documents/BigData/netflixrecommender/content.py "/Users/jamesledoux/Documents/BigData/netflixrecommender/movie_features_dataset.dat/"
conf = SparkConf().setAppName("KMeans-Content").set("spark.executor.memory", "7g")
sc = SparkContext()

if(len(sys.argv) != 2):
    print "usage: /sparkPath/bin/spark-submit  name.py  'movieDirectory'"

def parseRating(line):
    parts = line.strip().split("::")
    return (int(parts[0])-1, int(parts[1])-1, float(parts[2]))

#load in input file
path = sys.argv[1]

#path = "/Users/jamesledoux/Documents/BigData/netflixrecommender/movie_features_dataset.dat/"
data = MLUtils.loadLibSVMFile(sc, path)

labels = data.map(lambda x: x.label)
features = data.map(lambda x: x.features)


#normalize:
#scaler = StandardScaler(withMean = True, withStd = True).fit(features)  #data needs to be dense (zeros included)
scaler = StandardScaler(withMean = False, withStd = True).fit(features)  #becomes dense if using withMean. may run out of memory locally

#convert data to dense vector to be normalized
#data2 = labels.zip(scaler.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
data2 = labels.zip(scaler.transform(features))   #use this line if having memory issues

#hide 10% of the data for final test
data, test = data2.randomSplit([.9, .1])

#get size of chunks for 10-fold cross-validation
num_folds = 10
partitionSize = (len(data.collect())/num_folds)   #parameterize this value as num_folds (in loop as well)

#train/validate 10 times on each k
i = 0
j = partitionSize
data = data.collect()
cv_error_storage = []

#10 fold is better, but I use 5 here in the interest of time
for w in range(num_folds):
    #new train/validation split
    train = data[0:i] + data[j:]
    val = data[i:j]
    train = sc.parallelize(train)
    val = sc.parallelize(val)
    minError = float("inf")
    bestModel = None
    bestK = None
    test_values = [2,3,4,5,6,7,8,9,10]
    #test_values = [2,3]
    error_storage = []
    for x in test_values:
        model = KMeans.train(train.values(), x, maxIterations=10, runs=10, epsilon=.00001)
        error = model.computeCost(val.values())
        error_storage.append(error)
        print "******     model with " + str(x) + " clusters done in validation fold " + str(w+1) + " ***********"
        if error < minError:
            bestModel = model
            minError = error
            bestK = x
    cv_error_storage.append(error_storage)
    i = i + partitionSize
    j = j + partitionSize


CVerrors = []
for i in range(len(test_values)):
    val = np.mean(sum(val[i] for val in cv_error_storage))
    CVerrors.append(val)

minError = float('inf')
j = 0
for i in CVerrors:
    if i < minError:
        minError = i
        bestK = test_values[j]
    j = j+1

print '********    BEST VALUE OF K: ' + str(bestK) + "  *********"

data = sc.parallelize(data)
bestModel = KMeans.train(data.values(), bestK, maxIterations=10, runs=10, epsilon=.00001)
testError = model.computeCost(test.values())
print "test error: " + str(testError)



#now score model on the test data
bestModel = KMeans.train(data.values(), bestK, maxIterations=10, runs=10, epsilon=.00001)
error = model.computeCost(test.values())

print "best model with k = " + str(bestK) + " finished with error: " + str(error)

modelCenters = bestModel.clusterCenters

#get rdd of clusterid, movieid tuples  (predict after training)
trainingClusterLabels = train.map(lambda x: (bestModel.predict(x[1]), x[0]))

###################   example of this in action   ######################
#get recommendations for a user based on movies he/she liked
#add your own ratings data path to view this yourself

"""
path2 = "/Users/jamesledoux/Documents/BigData/netflixrecommender/ratings.dat" #make this a passed-in argument
ratings = sc.textFile(path2).map(parseRating)  #make the parse rating function
ratingsByUser = ratings.map(lambda x: (x[0], (x[1],x[2])))
ratingsByUser = ratingsByUser.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()

user = ratingsByUser[0]
userHighRatings = [movieRating for movieRating in user[1] if movieRating[1] == 5]

singleRating = userHighRatings[0]
clusterId = model.predict(data2.lookup(singleRating[0])[0])

#these are the recommended movies:
samplesInRelevantCluster = trainingClusterLabels.lookup(clusterId)
"""


plt.plot(CVerrors)
plt.xticks([0,1,2,3,4,5,6,7,8],test_values)
plt.show()


sc.stop()
