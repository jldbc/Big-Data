**Final Project: Gutenberg Recommender System**

* Data: the data comes from Project Gutenberg, and was extracted using the shell script GetBooks.sh
* Preprocessing: we extract a number of features to represent the content of the books. These features include punctuation frequencies, book length, author, and content clusters (using k-means clustering on TF-IDF scores to attain these)
* Clustering: we use AWS EMR to run k-means clustering on the data in Spark, in order to add content clusters to the books' feature profiles
* Recommendations: we serve end-recommendations using k-nearest-neighbors. The user enters a book, and the script returns the books with the most similar feature profiles (punctuation, author, book length, content clusters, etc.)

This is the version of the project that was submitted for the class. For a more up-to-date version that we have improved upon since, see [Gutenberg](http://github.com/jldbc/gutenberg).
