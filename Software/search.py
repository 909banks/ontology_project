import ontology_wrapper
import os

# The executable file needs to have extra quotationmarks to be able to be executed by the os.system command
GRAPH_EXECUTABLE=r'"C:\Users\Dan\AppData\Local\GraphDB Free\GraphDB Free.exe"'
GRAPH_URL="http://192.168.0.19:7200/repositories/company_ownership_ontology"

graphInterface=ontology_wrapper.Interface(GRAPH_URL)
graphInterface.connectToGraph(graphExecutable=GRAPH_EXECUTABLE, graphURL=GRAPH_URL)

# The fringe is the list of unexpanded nodes, when used the iterative deepening search it is implemented as a stack
fringe=[]
# The path is the list of people and companies returned to get from the start to the end goal
path=[]

def bidierctionalSearch():
    pass

def recursiveDLS(currentState, goalState, limit, search):
    if currentState == goalState:
        path.insert(0, currentState)
        return True
    elif limit==0:
        return "cutoff"
    else:
        cuttoffOccurred=False
        # The goal node has not yet been found and we are within the current set limit so we
        # iterate deeper, if there are no children then cutoff never occurred --> failure is returned
        children=graphInterface.queryOntology(currentState)
        for child in children:
            result=recursiveDLS(child, goalState, limit-1, search)
            # If the child is beyond the depth limit
            if result=="cutoff":
                cuttoffOccurred=True
            # If the child is the goal state, as the recursive function propagates up we add the current state to the front of the path
            elif result==True:
                path.insert(0, currentState)
                return result

        # If the goal state has not been found within the depth limit
        if cuttoffOccurred:
            return "cutoff"
        # If the goal has not been found and we are within the depth limit
        else:
            return False

def iterativeDeepening(currentState, goalState, maxDepth, search):
    for depth in range(0, maxDepth):
        result = recursiveDLS(currentState, goalState, depth, search)
        if result != "cutoff":
            return result

