from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkConf, SparkContext
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from matplotlib import pyplot as plt
import numpy as np
from os.path import join
import sys
#pyspark.tuning.ml.crossvalidator import?

if(len(sys.argv) != 2):
    print "usage: /sparkPath/bin/spark-submit  name.py  movieDirectory"

conf = SparkConf().setAppName("KMeans Collaborative").set("spark.executor.memory", "7g")
movieLensHomeDir = sys.argv[1]   # passed as argument
sc =SparkContext()


def parseRating(line):
    #uid::movieID::rating::timestamp
    parts = line.strip().split("::")
    return long(parts[3])%10, (int(parts[0])-1, int(parts[1])-1, float(parts[2]))  #parentheses probably wrong here

def loadRatings(sc, MLDir):
    return sc.textFile(join(MLDir, "ratings.dat")).map(parseRating)

def vectorize(ratings, numMovies):
    return ratings.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(lambda x: SparseVector(numMovies, x))

ratings = loadRatings(sc, movieLensHomeDir)
print "type of ratings obj: ", type(ratings)
print "count of ratings: ", len(ratings.collect())
print "sample rating: ", ratings.take(1)

#num of users  (userid, movieid, ratings)
numUsers = ratings.values().map(lambda x: x[0]).max()+1
numMovies = ratings.values().map(lambda x: x[1]).max()+1


ratingsSV = vectorize( ratings.values(), numMovies)
print "RatingsSV Type:", type(ratingsSV)
print "RatingsSV Count:", ratingsSV.count()

#cross validate
data, test = ratingsSV.randomSplit([.9, .1])
num_folds = 5   #make this a command line arg
partitionSize = (len(data.collect())/num_folds)

i = 0
j = partitionSize
data = data.collect()
cv_error_storage = []

for w in range(num_folds):
    #new train/validation split
    train = data[0:i] + data[j:]
    val = data[i:j]
    train = sc.parallelize(train)
    val = sc.parallelize(val)
    minError = float("inf")
    bestModel = None
    bestK = None
    test_values = [80, 90, 100, 110, 120, 130, 140]
    #test_values = [120]
    error_storage = []
    for x in test_values:
        model = KMeans.train(train.values(), x, maxIterations=10, runs=10, epsilon=.00001)
        error = model.computeCost(val.values())
        error_storage.append(error)
        print "******     model with " + str(x) + " clusters done in validation fold " + str(w+1) + " ***********"
        print "with error: " + str(error)
        if error < minError:
            bestModel = model
            minError = error
            bestK = x
    cv_error_storage.append(error_storage)
    i = i + partitionSize
    j = j + partitionSize


#get CVerrors (mean of the errors from the 10 cross validated samples)
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
print 'best k: ' + str(bestK)
plt.plot(CVerrors)
plt.show()


data = sc.parallelize(data)
bestModel = KMeans.train(data.values(), bestK, maxIterations=10, runs=10, epsilon=.00001)
testError = model.computeCost(test.values())


#get a sample prediction besed on this model
user = ratingsSV.values().take(5)[4] #take a sample of 1 from the data set (use test data when doing this)
print "Type user:", type(user)
print "User:", user

label = bestModel.predict(user)   #outputs which cluster this user belongs to
clusterCenters = model.clusterCenters     #len == total num of movies, each obs ==  rating for people in this group
#clusterCenters[0] #len == total num of movies, each obs == avg rating for people in this group
print "Len Cluster Centers:", len(clusterCenters)
movieID = 4

print "predicted value: ", clusterCenters[label][movieID]


sc.stop()
