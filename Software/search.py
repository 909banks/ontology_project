import ontology_wrapper
import file_interface
import os
import time
import random
import threading
import csv

# The executable file needs to have extra quotationmarks to be able to be executed by the os.system command
GRAPH_EXECUTABLE=r'"C:\Users\Dan\AppData\Local\GraphDB Free\GraphDB Free.exe"'
GRAPH_URL="http://192.168.0.19:7200/repositories/company_ownership_ontology"

# This is the utility interface for access to the ontology outside of the searches
graphInterface=ontology_wrapper.Interface(GRAPH_URL)
graphInterface.connectToGraph(graphExecutable=GRAPH_EXECUTABLE, graphURL=GRAPH_URL)

# The fringe is a nested dictionary of expanded and unexpanded nodes, it is implemented as a dictionary of
# string keys and another dictionary stating the parent and whether it has been explored
node = {"parentName":"",
        "companyID" : ""}
fringe={"nodeName":node}

# The path is the list of peaople and compnies used to get from the starting node to the goal node
# The path will be ordered such that it is a list of three strings, as shown below:
# Index     DirectorName        Company     Child's Company
# 0         SULLIVAN DANIEL J   CTG         SNDR
# 1         SULLIVAN DANIEL J   SNDR        
# 1         ....                ...         ...
path=[]

# Create a threading lock for use when the search needs to query the ontology
ontologyLock = threading.Lock()
killLock = threading.Lock()
killRequest = 0

def calcualteCost(node={str:str,str:str}):
    """
    This function calculates the estimated cost of the node that it is given, based on 
    the number of intermediaries between it and the starting node and the connectivity 
    of the node. This function uses the utility graph interface, so as not to interfere
    with the other searches expanded companies and filtering in place
    
    Keyword Arguments:
        node {dict} --  the name and company ID of the current node selected for cost 
                        estimation (default: {{str:str, str:str}})
    
    Returns:
        int -- The estimated cost of the node
    """
    # Aquire the ontology lock and get the connectivity of the next node
    ontologyLock.acquire()
    graphInterface.resetExpandedCompanies()
    connections=len(graphInterface.queryOntology(node))
    ontologyLock.release()

    cost = 1 - (connections/100)
    return cost

def constructPath(halfwayNode):
    parent = fringe[halfwayNode["name"]]["parentName"]
    while parent in fringe.keys():
        path[-1][2] = fringe[parent]["companyID"]
        path.append([ parent, fringe[parent]["companyID"], "N/A" ])
        parent = fringe[parent]["parentName"]

def breadthFirstSearch(ontologyInterface, currentNode={str:str,str:str}, goalNode=str, manageFringe=bool):
    """
    This function implements a breadth first search on the ontology
    
    Keyword Arguments:
        currentNode {dict} -- the name of the starting node (default: {{str:str,str:str,str:str}})
        goalNode {str} -- the name of the goal node (default: {str})
        manageFringe {bool} -- parameter used to determine if this search will add the unexpanded nodes to the fringe used in the bidierctional search (default: {bool})

    Returns:
        results {bool} -- returns True if solution found, returns False if there is no bath between the nodes
    """
    # Check if the starting node is the goal node
    if currentNode["name"] == goalNode:
        return True
    frontier=[currentNode]
    explored = {}

    while True:
        if frontier==[]:
            return False
        currentNode=frontier.pop()
        explored[currentNode["name"]] = currentNode
        # Aquire the ontology lock and return all the directors connected to the current node by one intermediate company
        ontologyLock.acquire()
        children=ontologyInterface.queryOntology(currentNode)
        ontologyLock.release()
        for child in children:
            child["parent"] = currentNode["name"]

            # Only check the child nodes if we have not previously explored them
            if not(child["name"] in frontier or child["name"] in explored.keys()):
                if child["name"] == goalNode:
                    # The goal node has been found, create the path from the goal node back to the start node
                    path.insert(0, [child["name"], child["companyID"], "N/A"])
                    parent = explored[child["parent"]]
                    while parent != None:
                        path.insert(0, [parent["name"], parent["companyID"], path[0][1]])
                        if "parent" in (explored[parent["name"]].keys()):
                            parent = explored[parent["parent"]]
                        else:
                            parent=None
                    return True
                
                # If not the goal node, add the child to the front of the frontier
                frontier.insert(0, child)

def recursiveDLS(ontologyInterface, currentNode={str:str,str:str}, goalNode=str, limit=int, manageFringe=bool):
    """
    Implement the recursive function of a deepening search
    
    Arguments:
        currentNode {dict} -- Denotes the name of the current node selected for expansion in the search (default: {{str:str, str:str}})
        goalNode {str}    -- Denotes the name of the goal node of the search (default: {str})
        limit {int}       -- The depth limit (how many layers down of expansion from this node left) (default: {int})
        search {bool}     -- Denotes which of the two searches being implemented we are in, used to know
                             whether to add to the fringe or not (default: {bool})
    
    Returns:
        results {bool/string} -- returns whether the search was successful, the search failed or reached a cutoff point
    """
    # Check if the other tread has finished the search
    global killRequest
    if killRequest == 1 or killLock.locked():
        exit()

    # Check if the current node is the goal node
    if currentNode["name"] == goalNode:
        path.insert(0, [currentNode["name"], currentNode["companyID"],"N/A"])
        return True

    # Check if the current node is in the fringe, if true then construct the path from the halfway point and return true
    elif currentNode["name"] in fringe.keys() and manageFringe == False:
        path.insert(0, [currentNode["name"], currentNode["companyID"], "temp"] )
        constructPath(currentNode)
        path[0][2] = path[1][1]

        # Flag the kill request for the other thread
        killLock.acquire()
        killRequest = 1
        killLock.release()
        return True

    # Check if we are at the cutoff
    elif limit==0:
        return "cutoff"
    else:
        cuttoffOccurred=False
        # The goal node has not yet been found and we are within the current set limit so we
        # iterate deeper, if there are no children then cutoff never occurred --> failure is returned
        ontologyLock.acquire()
        children=ontologyInterface.queryOntology(currentNode)
        ontologyLock.release()

        # Add the children to the fringe as they are discovered
        if manageFringe:
            for child in children:
                fringe[child["name"]] = {"parentName":currentNode["name"],
                                        "companyID": child["companyID"]}

        # Process the discovered children
        for child in children:
            result=recursiveDLS(ontologyInterface, child, goalNode, limit-1, manageFringe)
            # If the child is beyond the depth limit
            if result=="cutoff":
                cuttoffOccurred=True

            # If the child is the goal node, as the recursive function propagates up we add the current node to the front of the path
            elif result==True:
                path.insert(0, [currentNode["name"], currentNode["companyID"], path[0][1]])
                return result

        # If the goal node has not been found within the depth limit
        if cuttoffOccurred:
            return "cutoff"

        # If the goal has not been found and we are within the depth limit
        else:
            return False

def iterativeDeepening(ontologyInterface, startNode=str, goalNode=str, maxDepth=int, manageFringe=bool):
    """
    This function is used to implement a iterative deepening search on the ontology, 
    up to the maximum depth of the ontology.
    
    Arguments:
        startNode {str} -- Denotes the name of the starting node of the search (default: {str})
        goalNode {str}  -- Denotes the name of the goal node of the search (default: {str})
        maxDepth {int}  --  The maximum depth of the ontology, retrieved from the ontology connections file (default: {int})
        manageFringe {boolean}  --  Boolean variable used to indicate which of the two searches is currently being run (default: {bool})
    
    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was then the Path list will contain the optimal path
    """
    # If this search is managing the fringe, add the starting node to the fringe
    if manageFringe:
        fringe[startNode] = {"parentName": "",
                            "compnayID": "N/A"}

    startNode={"name":startNode, "companyID": "N/A"}
    for depth in range(1, maxDepth):
        result = recursiveDLS(ontologyInterface, startNode, goalNode, depth, manageFringe)
        if result != "cutoff":
            return result
        # Need to reset the expanded companies at each iteration
        ontologyInterface.resetExpandedCompanies()

def RBFS(ontologyInterface, currentNode={str:str, str:str}, goalNode=str, fLimit=int):
    """
    Implement the recursive function of the best first search
    
    Keyword Arguments:
        currentNode {dict}  --  Denotes the name of the current node selected for 
                                expansion in the search (default: {{str:str, str:str}})
        goalNode {str}      --  Denotes the name of the goal node of the search (default: {str})
        fLimit {int}        --  The maximum cost of the node we are willing to expand (default: {int})

    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was 
                            then the Path list will contain the optimal path between the nodes
    """
    # Check if the currentNode is the goal node
    if currentNode["name"] == goalNode:
        path.insert(0, [currentNode["name"], currentNode["compayID"], "N/A"] )
        return True
    
    # Aquire the ontology locka and return all the people connected to the current node by one intermidiate company
    ontologyLock.acquire()
    successors=ontologyInterface.queryOntology(currentNode)
    ontologyLock.release()
    if successors == []:
        return False
    
    # Set the cost of the sucessors to the maximum of their estimated cost and the cost of the current node
    successorCost={}
    for s in successors:
        successorCost[s["name"]] = calcualteCost(s)
    successorCost={k: v for k, v in sorted(successorCost.items(), key=lambda item: item[1])}

    while True:
        best, alternative = {k: successorCost[k] for k in list(successorCost)[:2]}
        if successorCost[best] > fLimit:
            return False
        node=([d for d in successors if d["name"]==best])[0]
        del successorCost[best]
        result = RBFS(ontologyInterface, currentNode=node, goalNode=goalNode, fLimit=min(successorCost[alternative], fLimit))
        if not(result == False):
            return result

def recursiveBestFirstSearch(ontologyInterface, startNode=str, goalNode=str, fLimit=int, manageFringe=bool):
    """
    This function implements a version of the best first search algorithm, the base cost is 1 as there is no actual distance between any node and the goal node
    
    Keyword Arguments:
        startNode {str} -- Denotes the name of the starting node of the search (default: {str})
        goalNode {str} -- Denotes the name of the goal node of the search (default: {str})
        fLimit {int} -- The Maximum cost of a node that we are willing to expand (default: {int})
        manageFringe {bool} -- Boolean variable used to indicate if this search should be adding unexplored nodes to the fringe (default: {bool})

    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was then the Path list will contain the optimal path
    """
    initialNode = {"name":startNode, "companyID":"N/A"}
    return RBFS(ontologyInterface, initialNode, goalNode, fLimit=100000)

def bidirectionalSearch():
    """
    The purpise of this function is to run two searches concurrently, one starting from the start node, the other from
    the goal node. With an aim to meet in the middle, in order to reduce the execution time of the search.

    At least one search must manage a fringe.
    """
    global path

    ontology_1=ontology_wrapper.Interface(GRAPH_URL)
    ontology_1.connectToGraph(graphExecutable=GRAPH_EXECUTABLE, graphURL=GRAPH_URL)
    ontology_2=ontology_wrapper.Interface(GRAPH_URL)

    startName="Pyle Robert D"
    names = file_interface.getOntologyNames()
    for i in range(len(names)-1):
        if i%20 == 0:
            time.sleep(5)
        goalName=names[i]
        if startName != goalName:
            print(startName + " --> " +goalName)
            # Start running two searches concurrently, with each search starting from the opposite end of the relationship
            t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalName, 4, False])
            #t2 = threading.Thread(target=iterativeDeepening, args=[ontology_2, goalName, startName, 100, True])
            t1.start()
            #t2.start()
            t1.join()
            #t2.join()
            if len(path) > 0:
                file_interface.writePath(path)
            ontology_1.resetExpandedCompanies()
            path = []
            fringe = {}

bidirectionalSearch()