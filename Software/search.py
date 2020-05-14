import ontology_wrapper
import file_interface
import time
import threading
import sys

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
possiblePath=[]
paths=[]

# Create a threading lock for use when the search needs to query the ontology
queryLock = threading.Lock()
killLock = threading.Lock()
killRequest = 0

def constructPath(halfwayNode):
    parent = fringe[halfwayNode["name"]]["parentName"]
    while parent in fringe.keys():
        possiblePath.append([ parent, possiblePath[-1][2], fringe[parent]["parentCompany"] ])
        parent = fringe[parent]["parentName"]
    possiblePath[-1][1] = possiblePath[-2][2]

def breadthFirstSearch(ontologyInterface, currentNode={"name":"N/A","compnayID":"N/A"}, goalNode="N/A", manageFringe=False):
    """
    This function implements a breadth first search on the ontology
    
    Keyword Arguments:
        currentNode {dict} -- the name of the starting node (default: {{str:str,str:str,str:str}})
        goalNode {str} -- the name of the goal node (default: {str})
        manageFringe {bool} -- parameter used to determine if this search will add the unexpanded nodes to the fringe used in the bidierctional search (default: {bool})

    Returns:
        results {bool} -- returns True if solution found, returns False if there is no bath between the nodes
    """
    global killRequest
    global possiblePath
    global paths
    # Check if the starting node is the goal node
    if currentNode["name"] == goalNode:
        return True
    frontier=[currentNode]
    explored = {}

    while True:
        # Check if the other tread has finished the search
        if killRequest == 1 or killLock.locked():
            sys.exit()

        # Check if we have explored all available nodes
        if frontier==[]:
            return False
        currentNode=frontier.pop()
        explored[currentNode["name"]] = currentNode
        
        # Aquire the ontology lock and return all the directors connected to the current node by one intermediate company
        queryLock.acquire()
        children=ontologyInterface.queryOntology(currentNode)
        queryLock.release()

        # Add the children to the fringe as they are discovered
        if manageFringe:
            for child in children:
                fringe[child["name"]] = {"parentName":currentNode["name"],
                                        "parentCompany": child["companyID"]}
        

        # For all of the children of the currently selected node, check if it is the goal node
        for child in children:
            child["parent"] = currentNode["name"]
            # Only check the child nodes if we have not previously explored them
            if not(child["name"] in frontier or child["name"] in explored.keys()):
                if child["name"] == goalNode:
                    # The goal node has been found, create the path from the goal node back to the start node
                    possiblePath.insert(0, [child["name"], child["companyID"], "N/A"])
                    parent = explored[child["parent"]]
                    while parent != None:
                        possiblePath.insert(0, [parent["name"], parent["companyID"], possiblePath[0][1]])
                        if "parent" in (explored[parent["name"]].keys()):
                            parent = explored[parent["parent"]]
                        else:
                            parent=None
                    return True

                # Check if the current node is in the fringe, if true then construct the path from the halfway point and return true
                elif child["name"] in fringe.keys() and manageFringe == False:
                    # Kill the thread if we have already found a path
                    if len(paths) > 1:
                        # Flag the kill request for the other thread
                        killLock.acquire()
                        killRequest = 1
                        killLock.release()

                    # Construct the end of the path from the halfway node
                    possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], fringe[currentNode["name"]]["parentCompany"] ] )
                    constructPath(currentNode)
                    possiblePath[0][2] = possiblePath[1][1] 

                    # Construct the path from the halfway point back to the start node
                    parent = explored[child["parent"]]
                    while parent != None:
                        possiblePath.insert(0, [parent["name"], parent["companyID"], possiblePath[0][1]])
                        if "parent" in (explored[parent["name"]].keys()):
                            parent = explored[parent["parent"]]
                        else:
                            parent=None
                    paths.append(possiblePath)
                    possiblePath = []
                
                # If not the goal node, add the child to the front of the frontier
                frontier.insert(0, child)

def recursiveDLS(ontologyInterface, currentNode={"name":"N/A","compnayID":"N/A"}, goalNode="N/A", limit=0, manageFringe=False):
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
    global possiblePath
    global paths
    if killRequest == 1 or killLock.locked():
        sys.exit()

    # Check if the current node is the goal node
    if currentNode["name"] == goalNode:
        possiblePath.insert(0, [currentNode["name"], currentNode["companyID"],"N/A"])
        return True

    # Check if the current node is in the fringe, if true then construct the path from the halfway point and return true
    elif currentNode["name"] in fringe.keys() and manageFringe == False:
        if len(paths) > 1:
            # Flag the kill request for the other thread
            killLock.acquire()
            killRequest = 1
            killLock.release()

        # Construct the end of the path from the other search
        possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], fringe[currentNode["name"]]["parentCompany"] ] )
        constructPath(currentNode)
        possiblePath[0][2] = possiblePath[1][1]
        paths.append(possiblePath)
        possiblePath=[]

    # Check if we are at the cutoff
    elif limit==0:
        return "cutoff"
    else:
        cuttoffOccurred=False
        # The goal node has not yet been found and we are within the current set limit so we
        # iterate deeper, if there are no children then cutoff never occurred --> failure is returned
        queryLock.acquire()
        children=ontologyInterface.queryOntology(currentNode)
        queryLock.release()

        # Add the children to the fringe as they are discovered
        if manageFringe:
            for child in children:
                fringe[child["name"]] = {"parentName":currentNode["name"],
                                        "parentCompany": child["companyID"]}

        # Process the discovered children
        for child in children:
            result=recursiveDLS(ontologyInterface, child, goalNode, limit-1, manageFringe)
            # If the child is beyond the depth limit
            if result=="cutoff":
                cuttoffOccurred=True

            # If the child is the goal node, as the recursive function propagates up we add the current node to the front of the path
            elif result==True:
                possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], possiblePath[0][1]])
                return result

        # If the goal node has not been found within the depth limit
        if cuttoffOccurred:
            return "cutoff"

        # If the goal has not been found and we are within the depth limit
        else:
            return False

def iterativeDeepening(ontologyInterface, startNode="N/A", goalNode="N/A", maxDepth=15, manageFringe=False):
    """
    This function is used to implement a iterative deepening search on the ontology, 
    up to the maximum depth of the ontology.
    
    Arguments:
        startNode {str} -- Denotes the name of the starting node of the search (default: {str})
        goalNode {str}  -- Denotes the name of the goal node of the search (default: {str})
        maxDepth {int}  --  The maximum depth of the ontology, retrieved from the ontology connections file (default: {int})
        manageFringe {bool}  --  Boolean variable used to indicate which of the two searches is currently being run (default: {bool})
    
    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was then the Path list will contain the optimal path
    """
    # If this search is managing the fringe, add the starting node to the fringe
    if manageFringe:
        fringe[startNode] = {"parentName": "",
                            "parentCompany": "N/A"}

    startNode={"name":startNode, "companyID": "N/A"}
    for depth in range(1, maxDepth):
        result = recursiveDLS(ontologyInterface, startNode, goalNode, depth, manageFringe)
        if result != "cutoff":
            return result
        # Need to reset the expanded companies at each iteration
        ontologyInterface.resetExpandedCompanies()

def calcualteCost(node={"name":"N/A","compnayID":"N/A"}, depth=0):
    """
    This function calculates the estimated cost of the node that it is given, based on 
    the number of intermediaries between it and the starting node and the connectivity 
    of the node. This function uses the utility graph interface, so as not to interfere
    with the other searches expanded companies and filtering in place
    
    Keyword Arguments:
        node {dict} --  the name and company ID of the current node selected for cost 
                        estimation (default: {{str:str, str:str}})
        depth {int} --  The current depth of the search (default: {int})
    
    Returns:
        int -- The estimated cost of the node
    """
    # Aquire the ontology lock and get the connectivity of the next node
    queryLock.acquire()
    graphInterface.resetExpandedCompanies()
    connections=len(graphInterface.queryOntology(node))
    queryLock.release()
    
    depthFactor = (1 - depth/6) if depth < 6 else 1
    connectionFactor = connections/50 
    cost = 1 - 0.6*connectionFactor + 0.4*depthFactor
    return cost

def RBFS(ontologyInterface, currentNode={"name":"N/A","companyID":"N/A"}, goalNode="N/A", fLimit=15, depth=0, parentCompanies=[], manageFringe=False):
    """
    Implement the recursive function of the best first search
    
    Keyword Arguments:
        currentNode {dict}  --  Denotes the name of the current node selected for 
                                expansion in the search (default: {{str:str, str:str}})
        goalNode {str}      --  Denotes the name of the goal node of the search (default: {str})
        fLimit {int}        --  The maximum cost of the node we are willing to expand (default: {int})
        manageFringe {bool} --  Boolean variable used to indicate which of the two searches is currently being run (default: {bool})

    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was 
                            then the Path list will contain the optimal path between the nodes
    """
    # Check if the other tread has finished the search
    global killRequest
    global possiblePath
    global paths
    if killRequest == 1 or killLock.locked():
        sys.exit()

    # Check if the currentNode is the goal node
    if currentNode["name"] == goalNode:
        possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], "N/A"] )
        return True
    # Check if the current node is in the fringe, if true then construct the path 
    # from the halfway point and return true
    elif currentNode["name"] in fringe.keys() and manageFringe == False:
        # Flag the kill request for the other thread
        killLock.acquire()
        killRequest = 1
        killLock.release()

        # Construct the end of the path from the other search
        possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], fringe[currentNode["name"]]["parentCompany"] ] )
        constructPath(currentNode)
        possiblePath[0][2] = possiblePath[1][1]
        paths.append(possiblePath)
        possiblePath=[]
    
    newList = parentCompanies[:]
    newList.append(currentNode["companyID"])
    ontologyInterface.setExpandedCompanies(newList)
    # Aquire the ontology locka and return all the people connected to the current 
    # node by one intermidiate company
    queryLock.acquire()
    successors=ontologyInterface.queryOntology(currentNode)
    queryLock.release()
    depth += 1
    if successors == []:
        return False, 1000000
    
    # Set the cost of the sucessors to the maximum between the cost of the current 
    # node and the sum of the cost so far (i.e. the depth) and their estimated cost
    for s in successors:
        s["cost"] = max(currentNode["cost"], depth+calcualteCost(s, depth))
        # If this search is required to manage the fringe, add all of the successors 
        # to the fringe as they are generated
        if manageFringe:
            fringe[s["name"]] = {"parentName":currentNode["name"],
                                "parentCompany": s["companyID"]}

    # Sort the successors into cost order
    successors=sorted(successors, key=lambda x: (x["cost"]))

    # Check if cost of all children is greater than the fLimit
    if successors[0]["cost"] > fLimit:
        return False, successors[0]["cost"]

    best=successors.pop(0)
    alternative=successors[0]
    while True:
        # Recursively explore the graph
        result = RBFS(ontologyInterface, best, goalNode, min(fLimit, alternative["cost"]), depth, newList, manageFringe)
        # If the goal node has been found, generate the path as the search ascends
        #  the recursion calls
        if result == True:
            possiblePath.insert(0, [currentNode["name"], currentNode["companyID"], possiblePath[0][1]] )
            return True
        elif result[1] != 1000000:
            # Set best node cost to the cost of their children
            best["cost"] = result[1]
            successors.append(best)
            successors=sorted(successors, key=lambda x: (x["cost"]))
        best = successors.pop(0)
        alternative=successors[0]
        if best["cost"] > fLimit:
            return False, best["cost"]

def recursiveBestFirstSearch(ontologyInterface, startName="N/A", goalName="N/A", fLimit=100, manageFringe=False):
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
    # If this search is managing the fringe, add the starting node to the fringe
    if manageFringe:
        fringe[startName] = {"parentName": "",
                            "companyID": "N/A"}
    initialNode = {"name":startName, "companyID":"N/A"}
    initialNode["cost"] = 0
    return RBFS(ontologyInterface, initialNode, goalName, fLimit=fLimit, depth=0, manageFringe=manageFringe)

def bidirectionalSearch():
    """
    The purpise of this function is to run two searches concurrently, one starting from the start node, the other from
    the goal node. With an aim to meet in the middle, in order to reduce the execution time of the search.

    At least one search must manage a fringe.
    """
    global possiblePath
    global paths
    global killRequest
    global fringe
    ontology_1=ontology_wrapper.Interface(GRAPH_URL)
    ontology_2=ontology_wrapper.Interface(GRAPH_URL)
    totalTime = 0

    startName = "Pyle Robert D"
    goalNames = ["Wallace Mark E",
            "WOLFE ROBERT H",
            "WOOD PHOEBE A",
            "WHITE MILES D",
            "HERNANDEZ ENRIQUE JR",
            "FABRIKANT CHARLES",
            "WEBSTER STEVEN A"]
    startNode={"name" : startName,"companyID" : "N/A"}
    for i in range(0, len(goalNames)):
        goalNode={"name" : goalNames[i],"companyID" : "N/A"}
        for _ in range(25):
            # Start running two searches concurrently, with each search starting from the opposite end of the relationship
            # Dual Iterative deepening searches
            searchA = "IDS"
            searchB = "IDS"
            t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalNames[i], 10, False])
            t2 = threading.Thread(target=iterativeDeepening, args=[ontology_2, goalNames[i], startName, 10, True])
            # # Dual breadth first searches
            # searchA = "BFS"
            # searchB = "BFS"
            # t1 = threading.Thread(target=breadthFirstSearch, args=[ontology_1, startNode, goalNames[i], False])
            # t2 = threading.Thread(target=breadthFirstSearch, args=[ontology_2, goalNode, startName, True])
            # # Dual best first searches
            # searchA = "RBFS"
            # searchB = "RBFS"
            # t1 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_1, startName, goalNames[i], 10, False])
            # t2 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_2, goalNames[i], startName, 10, True])
            # # 1 IDS 1 BFS
            # searchA = "IDS"
            # searchB = "BFS"
            # t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalNames[i], 10, False])
            # t2 = threading.Thread(target=breadthFirstSearch, args=[ontology_2, goalNode, startName, True])
            # t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalNames[i], 10, True])
            # t2 = threading.Thread(target=breadthFirstSearch, args=[ontology_2, goalNode, startName, False])
            # # 1 IDS 1 RBFS
            # searchA = "IDS"
            # searchB = "RBFS"
            # t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalNames[i], 10, False])
            # t2 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_2, goalNames[i], startName, 10, True])
            # t1 = threading.Thread(target=iterativeDeepening, args=[ontology_1, startName, goalNames[i], 10, True])
            # t2 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_2, goalNames[i], startName, 10, False])
            # # 1 BFS 1 RBFS
            # searchA = "BFS"
            # searchB = "RBFS"
            # t1 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_1, startName, goalNames[i], 10, False])
            # t2 = threading.Thread(target=breadthFirstSearch, args=[ontology_2, goalNode, startName, True])
            # t1 = threading.Thread(target=recursiveBestFirstSearch, args=[ontology_1, startName, goalNames[i], 10, True])
            # t2 = threading.Thread(target=breadthFirstSearch, args=[ontology_2, goalNode, startName, False])
            startTime = time.time()
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            totalTime += (time.time() - startTime)
            paths.append(possiblePath)
            ontology_1.resetExpandedCompanies()
            ontology_2.resetExpandedCompanies()
            possiblePath=[]
            paths=[]
            fringe={}
            killRequest=0

        averageTime = totalTime/25
        file_interface.writeSearchTimes(searchA, searchB, averageTime)
        time.sleep(10)

def singleSearch():
    global paths
    global possiblePath
    global fringe
    ontology_1=ontology_wrapper.Interface(GRAPH_URL)
    totalTime = 0
    search = ""
    startName = "Pyle Robert D"
    goalNames = ["Wallace Mark E",
                "WOLFE ROBERT H",
                "WOOD PHOEBE A",
                "WHITE MILES D",
                "HERNANDEZ ENRIQUE JR",
                "FABRIKANT CHARLES",
                "WEBSTER STEVEN A"]
    startNode={"name" : startName,"companyID" : "N/A"}
    goalNode={"name" : goalNames[0],"companyID" : "N/A"}
    for i in range(0,len(goalNames)):
        for _ in range(25):
            startTime = time.time()
            # # Iterarative Deepening Search
            # iterativeDeepening(ontology_1, startName, goalNames[i], 10, False)
            # search = "IDS"
            # # Breadth First Search
            # breadthFirstSearch(ontology_1, startNode, goalNames[i], False)
            # search = "BFS"
            # Best First Search
            recursiveBestFirstSearch(ontology_1, startName, goalNames[i], 10, False)
            search = "RBFS"
            totalTime += (time.time() - startTime)
            paths.append(possiblePath)
            ontology_1.resetExpandedCompanies()
            possiblePath=[]
            paths = []
        averageTime = totalTime/25
        file_interface.writeSearchTimes(search, i+2, averageTime)
        time.sleep(10)

if __name__=="__main__":
    bidirectionalSearch()
    # singleSearch()