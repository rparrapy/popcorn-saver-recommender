# Popcorn Saver Recommender

Popcorn Saver is a movie recommendation web application developed as a project for 2IMW15: Web Information Retrieval and Data Mining, during the first quartile of the 2015/2016 academic year at TU Eindhoven. It was developed using the [Movielens 100K](http://grouplens.org/datasets/movielens/) dataset.

This code corresponds to the web recommender, which provides Mahout-based recommendation API.

### Requirements
* A running instance of Apache Mahout. Installers can be found here: http://mahout.apache.org/general/downloads.html
* Java JDK v1.8
* Maven
### Installation
1. Clone the project.
```sh
$ git clone https://github.com/rparrapy/popcorn-saver-recommender.git && cd popcorn-saver-recommender
```
2. Run the server.
```sh
$ mvn exec:java -Dexec.mainClass="com.webir.popcornsaver.RestRecommender"
```

The recommendation server should be running on port 7000.