
Team: Drew Hoo, James LeDoux, Aniket Saoji

Instructions for running:
k means content:  spark-submit filename.py  “path_to_data”  
k means collaborative:  spark-submit filename.py  “path_to_data”  
knn collaborative: spark-submit filename.py “path to ratings data” “path to movies data”


*NOTE: K Means collaborative had a very high optimal k. Because of this, the code will take a long time to run. We are submitting with the optimal k being tested, but if you want to run the code simply to verify that it runs, we recommend changing the line “test_values = [n,n,n,n]” at line 65 to use smaller values of k.


*K Means Content*
After loading in the data, we normalize it by scaling the features to unit variance. We do not de-mean the data because this made the runtime much worse on our machines. We then set a random 10% of the data aside that we will use for a final model score after training the models. Of the remaining 90%, we run n-fold cross validation on k-means models, testing n models for each value of  k we have stored in the list “test_values”. We use a sliding window for our cross validation, where we move the indexes of our validation data forward by (data size/number of folds) observations on each iteration of the cross validation, so that each of our chunks are the validation set one time during the process, and a part of the training data on every other iteration. We then take the mean error value for each k tested from the n iterations of cross validation, and that is our cross-validation error (stored in the code as CVerrors). Finally, we select the lowest value from this as our “best k”. From here, we get the test error on a model with the 90% chunk of the data set as training data, and the other 10% as test data.  We then plot the CV errors using matplotlib, which are attached to the submission.

*K Means Collaborative*
We begin by loading in the ratings, and using these to create a sparse vector of movie:rating pairs by the users. We then cross validate the same way we did in the content version: by creating a 90/10 training/test split, then running n folds of the kmeans algorithm testing models with each k in “test_values” n times on different training/validation splits. We then take the best cross-validated error (mean error of a k-value from the n folds) and use this to get the final model and test error. We plot the cross-validated errors associated with each value of k.


*K Nearest Neighbors Collaborative*
First we load the ratings. We then create a dictionary of users, where the keys are user ids and the values are dictionaries of the users movie:rating pairs. We then calculate the distance between users (the get_distance() function) by measuring the mean squared difference between the ratings of movies that both users have rated. We also add a penalty to the MSE equal to (5/ the number of movies both had viewed) as a way of penalizing neighbors who appear artificially close by only having a small number of movies in common. We then sort the list of neighbors by distance, which gets a list of the user’s closest neighbors as measured by MSE. To get the k nearest neighbors, we select the first k from this sorted list. From these neighbors, we can then serve recommendations by selecting the highest rated movies from this set of similar users. This is what the predicted_movies() function does. We find the optimal k by hiding a subset of a user’s ratings and setting them to zero before calculating the user’s nearest neighbors, and then getting the mean squared distance between the predicted ratings and the real ones that we hid. 

*Results:*
The optimal k for k means collaborative was roughly 120. This took a long time to train during cross validation, so we were not able to achieve a level of precision closer than to the tens level, as the values we tested were [80, 90, 100, 110, 120, 130, 140]. The error score at k=120 was 8686924. 

The optimal k for k means content was 7, with an error score of 7345189.

The optimal k for KNN was 91, with an error score of 1.402 (RMSE).

The ALS approach from the data bricks tutorial achieved an error of 0.869148 (RMSE). 

These error numbers are not apples-to-apples comparisons because they measure squared differences of different things. The k means errors are squared distances between vectors, and the k nearest neighbors errors are squared differences between predicted movie ratings. They both work for determining optimal values of k and measuring performance, but they do not compare directly to one another. 

That said, we preferred KNN because the ratings for individual users appeared to be better in our testing. The way that KNN finds the nearest neighbors to an individual user (single-user centric) appears to serve better recommendations than the K-Means method of finding the cluster that is nearest to a user, and then assuming that the user’s preferences are the preferences of the rest of the cluster (other-users centric). Even this, however, did not perform as well as the ALS approach used in the DataBricks tutorial, whose RMSE was much smaller than our KNN’s RMSE. 

