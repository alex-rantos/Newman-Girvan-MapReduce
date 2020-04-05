# Databricks notebook source
vertices = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)], ["id", "name", "age"])

edges = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "a", "friend"),
  ("a", "c", "friend"),  
  ("c", "a", "friend"),
  ("b", "c", "friend"),
  ("c", "b", "friend"),
  ("b", "d", "friend"),
  ("d", "b", "friend"),
  ("d", "e", "friend"),
  ("e", "d", "friend"),
  ("d", "g", "friend"),
  ("g", "d", "friend"),
  ("e", "f", "friend"),
  ("f", "e", "friend"),
  ("g", "f", "friend"),
  ("f", "g", "friend"),
  ("d", "f", "friend"),
  ("f", "d", "friend")
], ["src", "dst", "relationship"])
vertices = vertices.drop("name","age")

from graphframes import *

graph = GraphFrame(vertices, edges)
display(graph.edges)

edges= edges.drop('relationship')

# COMMAND ----------

e=edges.rdd.map(lambda  x:(x[0],x[1]))
adjList=e.groupByKey().map(lambda x : (x[0], list(x[1])))
adjDict = adjList.collectAsMap() # make a adjacency dictionary

# make a dictionary of key-sets values.
adjDictSet = {}
for k in adjDict:
  adjDictSet[k] = set(adjDict[k])
# broadcast it so every worker can read it since no write operation will be performed
adjBroad = sc.broadcast(adjDictSet)
def getAdjOf(letter):
    return adjBroad.value[letter] 

# COMMAND ----------

def splitSeqByDelimiter(seq, delimiter):
    sublist = []
    for elem in seq:
        if (elem != delimiter):
            sublist.append(elem)
        else:
            yield sublist
            sublist = []
    # When delimiter was not found in the end, the last seq was not returned.
    if sublist:
        yield sublist     
        
def traverseNode(key,val):
  """
  k = (currentId,sourceId)
  v = (currentId,[sourceId,distance,visited,pathSum,pathList])
           currentId   [the nodeId that we are currently traversing]
  arr[0] = sourceId    [the nodeId from which BFS has started]
  arr[1] = distance    [int | Distance between targetId and sourceId]
  arr[2] = visited     [boolean| False if this node has not been expanded otherwise true]
  arr[3] = pathSum     [int | Number of shortest paths from sourceId to currentId]
  arr[4] = pathList    [List| list of visited nodes to reach k node]
  """
  k = val[0]
  v = val[1]
  src = val[1][0]
  returnRows = []
  if (v[2] == False):
    # set node to visited
    v[2] = True
    """
    A tuple with more than 5 elements means that on reduce stage
    we found 2 or more shortest paths  
    """
    if (len(v) > 5):
      v.pop()
      sublist = splitSeqByDelimiter(v[4],"*")
      print(sublist)
      for l in sublist:
        l.append(k)
        returnRows.append((key,(k,[v[0],v[1],v[2],v[3],l.copy()])))
        for a in (getAdjOf(k) - set(l)):
          returnRows.append(((a,src),(a,[v[0],v[1]+1,False,v[3],l.copy()])))
    else:
      # append current visited Node to pathList
      v[4].append(k)
      
      # emit tuple
      returnRows.append((key,val))

      # Get the nodes that are k's neighbors but have not been visited before
      for a in (getAdjOf(k) - set(v[4])):
        # emit each new path that can be discoved by visiting each Neighbor
        returnRows.append(((a,src),(a,[v[0],v[1] + 1,False,v[3],v[4].copy()])))
  else:
    # check again if any tuple contains two paths
    if (len(v) > 5):
      v.pop()
      sublist = splitSeqByDelimiter(v[4],"*")
      print(sublist)
      for l in sublist:
        returnRows.append((key,(k,[v[0],v[1],v[2],v[3],l.copy()])))
    else:
      # do nothing - emit tuple
      returnRows.append((key,val))
  return (returnRows)

def getLowestDistance(x,y):
  """
  Return the pair with the minimum pathSum thus returning the shortest Path
  If two pairs have the same pathSum merge their pathList and add one
  to the pathSum 
  """
  if (x[1][1] == y[1][1]):
    # make a single list with both paths with an "*" as delimiter for map stage
    return ((x[0],[x[1][0],x[1][1],x[1][2],x[1][3] + 1,x[1][4].copy() + ["*"] + y[1][4].copy(),False]))
  if (x[1][1] > y[1][1]):
    return y
  else:
    return x

# COMMAND ----------

# Perform BFS from every node of the graph.
# Each Iteration explores the graph an extra level till all nodes have been visited.
# Using flatMap because we generate new tuples
BFS = vertices.rdd.flatMap(lambda x:traverseNode((x[0],x[0]),(x[0],[x[0],0,False,1,[]])))
# loop until all nodes are visited
while(BFS.filter(lambda a: a[1][1][2] == False).count() > 0):
  BFS = BFS.flatMap(lambda x:traverseNode(x[0],x[1])).reduceByKey(lambda x,y: getLowestDistance(x,y))

# COMMAND ----------

def calculateBetwenness(v):
  """
  For each edge calculate its betwenness for the current shortest path (v)
  """
  returnRows = []
  for c,y in enumerate(v[4]):
    if (c == len(v[4]) - 1):
      break
    nextElem = v[4][c+1]
    if (nextElem < y):
      returnRows.append(((nextElem,y),1))
    else:
      returnRows.append(((y,nextElem),1))
  return (returnRows)

# COMMAND ----------

# flatMap(traverseNode) once again to check if the last reduce stage generated more than 1 shortest path. 
edgeValues = BFS.flatMap(lambda x:traverseNode(x[0],x[1])).flatMap(lambda x: calculateBetwenness(x[1][1])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0],x[1]/2))
edgeValues.collect()

# COMMAND ----------

betwenness_values = edgeValues.map(lambda x:x[1])
import statistics
maxBetwennessToDrop = max(betwenness_values.collect()) - statistics.stdev(betwenness_values.collect())
print(maxBetwennessToDrop)

# COMMAND ----------

edgesToDrop = edgeValues.filter(lambda x: x[1] >= maxBetwennessToDrop).map(lambda x:x[0]).collect()  

# COMMAND ----------

for x in edgesToDrop:
  if x[0] in adjDict:
    adjDict[x[0]].remove(x[1])
  if x[1] in adjDict:
    adjDict[x[1]].remove(x[0])

# COMMAND ----------

def deleteEdges(edge,listOfEdges):
  if (edge in listOfEdges or (edge[1],edge[0]) in listOfEdges):
    return False
  return True   

# COMMAND ----------

newEdges = edges.drop("relationship")
newEdges.rdd.filter(lambda x: deleteEdges(x,edgesToDrop)).collect()
sc.setCheckpointDir("dbfs:/tmp/groupEX/checkpoints")
newGraph = GraphFrame(vertices, newEdges)
newConnectedComponents = newGraph.connectedComponents()
newConnectedComponents = newConnectedComponents.groupBy("component").count().orderBy("count", ascending=False)
# create new communities
numOfCommunities = newGraph.labelPropagation(maxIter=5)
numOfCommunities.select("id", "label").show()

# COMMAND ----------

# display the number of different communities
numOfCommunities.select("label").distinct().count()

# COMMAND ----------

display(newGraph.edges)
