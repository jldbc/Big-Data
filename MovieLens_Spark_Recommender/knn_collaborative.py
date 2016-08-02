from __future__ import division
import sys
import itertools
from math import sqrt
import numpy as np
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
# How to run:
# /Users/jamesledoux/spark-1.6.1/bin/spark-submit /Users/jamesledoux/Documents/BigData/netflixrecommender/knn_collaborative.py "/Users/jamesledoux/Documents/BigData/netflixrecommender/ratings.dat" "/Users/jamesledoux/Documents/BigData/netflixrecommender/ratings.dat" moviesDir = "/Users/jamesledoux/Documents/Big Data/netflixrecommender/movies.dat"

# Generic Run
ratingsDir = sys.argv[1]
moviesDir = sys.argv[2]
######################################################################################
conf = SparkConf().setAppName("MovieLensKNN").set("spark.executor.memory", "7g")
sc = SparkContext(conf=conf)

def parseRating(line):
    fields = line.strip().split("::")
    return long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

def parseMovie(line):
    fields = line.split("::")
    return int(fields[0]), fields[1]

def loadRatings(ratingsFile):
    if not isfile(ratingsFile):
        print "File %s does not exist." % ratingsFile
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print "No ratings provided."
        sys.exit(1)
    else:
        return ratings

#mean squared distance between the movie ratings two users have in common
def get_distance(user1, user2):
    MSE_list = []
    common_movies = 0
    distances = []
    for movie in user1:
        if movie in user2:
            distances.append((user1[movie] - user2[movie])**2)
            common_movies += 1
    if common_movies == 0:
        MSE = float("inf")    #nothing in common, so max out the distance
        MSE_list.append(0)
        MSE_list.append(MSE)
    else:
        temp = np.mean(distances)
        MSE_list.append(temp)
        MSE = temp + 5/common_movies   #MSE + penalty for having fewer common movies
        MSE_list.append(MSE)
    #print(MSE_list)
    return MSE_list # MSE_list format: [Clean Error, Adjusted Error]

def predicted_movies(knn):
    """
    for movie in knn rating history, get an adjusted mean rating and return these
    sorted greatest to least
    """
    movies = {}
    k = len(knn)
    for i in knn:
        for movie in i.values()[0][1]:
            if movie in movies:
                movies[movie].append(i.values()[0][1][movie])
            else:
                movies[movie] = [i.values()[0][1][movie]]
    #mean val + a reward for movies seen by multiple neighbors (max poss. 1 pt. boost)
    for movie in movies:
        movies[movie] = np.mean(movies[movie]) + len(movies[movie])/k  #rationale: if more neighbors saw it and liked it, it is probably a better recommendation
    recommendations = []
    for i in movies:
        recommendations.append({i: movies[i]})
    recommendations = sorted(movies, key=lambda x: movies[x], reverse = True)  #movies w/ best avg score rated highest
    return recommendations, movies

def find_best_k(errors_dict): # Given a dictionary {key=k:val=total_error}
    keys = errors_dict.keys()
    for key in keys:
        temp = errors_dict[key]
        errors_dict[key] = temp/key
    print(errors_dict)
    return min(errors_dict, key=errors_dict.get)

def best_k_knn(user_id, k): # take the new test user and predict on blacked out vals
    # pick first two rated movies, and then set to zero
    neighbors = []
    user_ratings = users[user_id]   # a dict of movie: rating pairs {key=movie, val=rating}
    store_movie = {}
    # Creat test_user'
    key_list = user_ratings.keys()
    # Store the first n movies
    for i in range(6):
        store_movie[key_list[i]] = user_ratings[key_list[i]]
        user_ratings[key_list[i]] = 0

    k_total_error = []
    for user in users:
        if user != user_id:   #if not the user you are finding neighbors for
            ratings = users[user]
            dist = {}
            dist[user] = (get_distance(ratings, user_ratings)[1], ratings)
            neighbors.append(dist)
    #print(neighbors[0:3])
    nearest_neighbors = sorted(neighbors, key=lambda x: x.values()[0][0])
    knn = nearest_neighbors[0:k] #format => [{user_id: (distance, {movie:rating, movie:rating,...})}]
    # for each nearest neighbor, add the distances, where distance = MSE
    #print(knn)
    return store_movie, knn

#(person id, movie id, rating out of 5)
ratings = loadRatings(ratingsDir)

"""
Create dict of users and their movie ratings
Each user is a key. Each user's value is a dictionary, whose keys are movies and values are ratings.
{user: {movie: rating, movie: rating}, user: {movie: rating}, user: {}}
"""
users = {}
for (user, movie, rating) in ratings:
    if user not in users:
        users[user] = {}
        users[user][movie] = rating
    else:
        users[user][movie] = rating

user_id = 13   #a test user. parameterize this later.
k = [2, 3, 4, 5]  #also parameterize this
errors_dict = {} #format {key=k,val=MSE sum}


for x in k:
    print"for", x, "nearest neighbors"
    #[ { user_id: (distance, {movie: rating, movie: rating}), user_id: (dist, {movie: rating })}]
    neighbors = []
    user_ratings = users[user_id]   # a dict of movie: rating pairs {key=movie, val=rating}

    k_total_error = []
    for user in users:
        if user != user_id:   #if not the user you are finding neighbors for
            ratings = users[user]
            dist = {}
            dist[user] = (get_distance(ratings, user_ratings)[1], ratings)
            neighbors.append(dist)

    nearest_neighbors = sorted(neighbors, key=lambda x: x.values()[0][0])
    knn = nearest_neighbors[0:x] #format => [{user_id: (distance, {movie:rating, movie:rating})}]

    # for each nearest neighbor, add the distances, where distance = MSE
    k_MSE = 0
    for i in range(x):
        neighbor_id = knn[i].keys()
        MSE_neighbor = knn[i][neighbor_id[0]][0]
        k_MSE = k_MSE + MSE_neighbor
    print"Total Error for this k: ", k_MSE
    errors_dict[x] = k_MSE

print"dictionary of Errors pre best: ", errors_dict
best_k = find_best_k(errors_dict)


test_k_values = [85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95]
error_storage = []
for i in test_k_values:
    hidden_movies, knn = best_k_knn(user_id, i)
    #get top 8 recommendations based on k-nearest neighbors
    #movies = dict(sc.textFile(moviesDir).map(parseMovie).collect())
    #get the n top movies from your k nearest neighbors
    n = 8   #number of recommendations you want to see
    all_preds, movieDict = predicted_movies(knn)   #a list.. get from here to the rating somehow
    recs = all_preds[0:n]
    mse_store = []
    predictedRating = 0
    for key in hidden_movies.keys():
        try:
            predictedRating = movieDict[key]
        except:
            pass
        diff = (hidden_movies[key] - predictedRating)**2
        mse_store.append(diff)
    MSE_final = np.mean(mse_store)
    error_storage.append(MSE_final)
    print MSE_final



plt.plot(error_storage)
plt.xticks([0,1,2,3,4,5,6,7,8,9,10],test_k_values)
plt.show()


sc.stop()
