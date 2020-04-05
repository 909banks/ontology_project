import ontology_wrapper
import os

# The executable file needs to have extra quotationmarks to be able to be executed by the os.system command
GRAPH_EXECUTABLE=r'"C:\Users\Dan\AppData\Local\GraphDB Free\GraphDB Free.exe"'
GRAPH_URL="http://192.168.0.19:7200/repositories/company_ownership_ontology"

graphInterface=ontology_wrapper.Interface(GRAPH_URL)
graphInterface.connectToGraph(graphExecutable=GRAPH_EXECUTABLE, graphURL=GRAPH_URL)

# The fringe is a nested dictionary of expanded and unexpanded nodes, it is implemented as a dictionary of
# string keys and another dictionary stating the parent and whether it has been explored
node = {"parent":"",
        "explored":False}
fringe={"currentNode":node}

# The path is the list of peaople and compnies used to get from the starting node to the goal node
# The path will be ordered such that it is a list of three strings, as shown below:
# Index     DirectorName        Company     Child's Company
# 0         SULLIVAN DANIEL J   CTG         SNDR
# 1         SULLIVAN DANIEL J   SNDR        
# 1         ....                ...         ...
path=[]

def breadthFirstSearch(currentNode={str:str,str:str}, goalNode=str, manageFringe=bool):
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
        children=graphInterface.queryOntology(currentNode)
        for child in children:
            child["parent"] = currentNode["name"]
            if not(child in frontier or child["name"] in explored.keys()):
                if child["name"] == goalNode:
                    # How do we find the parent of the current node?
                    path.insert(0, [child["name"], child["companyID"], "N/A"])
                    parent = explored[child["parent"]]
                    while parent != None:
                        path.insert(0, [parent["name"], parent["companyID"], path[0][1]])
                        if "parent" in (explored[parent["name"]].keys()):
                            parent = explored[parent["parent"]]
                        else:
                            parent=None
                    return True
                frontier.insert(0, child)

def recursiveDLS(currentNode={str:str,str:str}, goalNode=str, limit=int, search=bool):
    """
    Implement the recursive function of a deepening search
    
    Arguments:
        currentNode {dict} -- Denotes the name of the current node selected for expansion in the search (default: {str})
        goalNode {str}    -- Denotes the name of the goal node of the search (default: {str})
        limit {int}       -- The depth limit (how many layers down of expansion from this node left) (default: {int})
        search {bool}     -- Denotes which of the two searches being implemented we are in, used to know
                             whether to add to the fringe or not (default: {bool})
    
    Returns:
        results {bool/string} -- returns whether the search was successful, the search failed or reached a cutoff point
    """
    if currentNode["name"] == goalNode:
        path.insert(0, [currentNode["name"], currentNode["companyID"],"N/A"])
        return True
    elif limit==0:
        return "cutoff"
    else:
        cuttoffOccurred=False
        # The goal node has not yet been found and we are within the current set limit so we
        # iterate deeper, if there are no children then cutoff never occurred --> failure is returned
        children=graphInterface.queryOntology(currentNode)
        for child in children:
            result=recursiveDLS(child, goalNode, limit-1, search)
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

def iterativeDeepening(currentNode=str, goalNode=str, maxDepth=int, search=bool):
    """
    This function is used to implement a iterative deepening search on the ontology, up to the maximum depth of the ontology.
    
    Arguments:
        currentNode {string} -- Denotes the name of the starting node of the search (default: {str})
        goalNode {string}    -- Denotes the name of the goal node of the search (default: {str})
        maxDepth {int}   -- The maximum depth of the ontology, retrieved from the ontology connections file (default: {int})
        search {boolean}     -- Boolean variable used to indicate which of the two searches is currently being run (default: {bool})
    
    Returns:
        result {boolean} -- Returns whether or not the search was successful, if it was then the Path list will contain the optimal path
    """
    startNode={"name":currentNode, "companyID": "N/A"}
    for depth in range(0, maxDepth):
        result = recursiveDLS(startNode, goalNode, depth, search)
        if result != "cutoff":
            return result

breadthFirstSearch({"name":"Jackson Spencer D.", "companyID":"N/A"}, "Shah Amit")
print(path)