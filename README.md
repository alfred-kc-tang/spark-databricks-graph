# Analysing Graph of Interactions on MathOverflow using Spark on Databricks

This project aims at tracking down which user has the most interactions in terms of asking or answering questions on the mathematics quesiton-and-answer (Q&A) website, [MathOverflow](https://mathoverflow.net/). Specifically, (1) the top 3 questioners, (2) the top 3 answerers, (3) the top 3 users that has the most interactions, and (4) the top 5 questioner-answer pairs (i.e. both asking and answering questions) will be spotted. Summary on these large amounts of data is also made by aggregating the number of the interactions by month.

All operations in this project are performed using Scala DataFrames.

## Table of Contents

* [Data Source](#data-source)
* [Keywords](#keywords)
* [Style Guide](#style-guide)
* [License](#license) 

## Data Source

The data set contains a temporal network of interactions on MathOverflow. With more than 100,000 records, each line in the data file represents a question asked and answered on the forum. The data is stored in a comma-separated-values (csv) file, in which each line has three values: (1) ID of the source node representing a user, (2) ID of the targer node representing another user, and (3) Unix timestamp in seconds since the epoch.

## References

Databricks, [Introduction to DataFrames - Scala](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html).

## Keywords

Apache Spark; Databricks; Edge; Graph Analytics; Node; Scala DataFrames.

## Style Guide

This project follows Scala official [style guide](https://docs.scala-lang.org/style/).

## License

This repository is covered under the [MIT License](https://github.com/alfred-kctang/spark-databricks-graph/blob/master/LICENSE).
