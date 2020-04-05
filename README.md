# About
A MapReduce approach to Newman-Girvan algorithm, which detects communities in a graph and removes the edge with the highest betweenness. Here all edges, above a dynamic threshold
<code> max_betweenness - standard_deviation_of_all_betweenness_values</code>,
are removed.

# Implementation
Firstly, BFS is executed from every node and explores all its non-visited neighbors. So the amount of BFS algorithms running in pararrel on the same graph is equal to the size of the graph. In order for this to work, each parent node must transfer his state to its children. 

The corresponding optimized BFS code:
```
while(BFS.filter(lambda a: a[1][1][2] == False).count() > 0):
    BFS = BFS.flatMap(lambda x:traverseNode(x[0],x[1])).reduceByKey(lambda x,y: getLowestDistance(x,y))
```
1. flatMap():   Expands the frontiers for the current node and produce new rows for the RDD.
2. reduceByKey(): Deletes all the non shortest paths. If two or more paths are the shortests, all are kept for the next iteration.
3. while: Iterate till all nodes from all BFS's are visited.

Code is thoroughly documented. 

Further optimization can be done by cacheing.

# Technologies
This project was developed in Databricks 6.3 & Apache Spark 2.4.4.

# Dependencies
```
pip install pyspark
pip install graphframes
```