import csv
import os

PATH_FILE = "search_path.csv"
ONTOLOGY_CONNECTIONS = "ontology_details.csv"
NAMES_FILE = "ontology-names.csv"

def writePath(pathArray:[[str,str,str]]):
    '''
    File used to write the path returned by the search algorithm, from the two parties of intrest

    Parameters:
    pathArray (2D-array [ [str] ]) - Path_lengthx3 array that describes the path from the start to the end goal state

    Returns:
    None
    '''
    ## Check if the path file exists
    ## If not write in the header
    if not os.path.exists(PATH_FILE):
        with open(PATH_FILE, mode='w',newline='') as pathFile:
            headerWriter=csv.writer(pathFile,delimiter=',')
            headerWriter.writerow(["Director", "Company Name", "Company Name 2"])

    ## For each element found in the path from person1 to person2, write the
    ## name of the intermediary, the company they work at with the previous 
    ## connection, and the new company
    intermediaries=len(pathArray)
    with open(PATH_FILE, mode='a',newline='') as pathFile:
        pathWriter=csv.writer(pathFile,delimiter=',')
        pathWriter.writerow([pathArray[0][0],"-->", pathArray[intermediaries-1][0]])
        for entity in pathArray:
            pathWriter.writerow([entity[0],entity[1],entity[2]])
        pathWriter.writerow([])

def writeOntologyConnections(relationships:{}):
    '''
    Function used to define the types of relationships within the ontology, this data
    will be used to find the mean average number of intermediaries and discern if our
    relationship is uncommon

    Parameters:
    relationships ( [int] ) - Array that counts the number of relationships within the ontology with 1,2,... intermediaries

    Returns:
    None
    '''
    ## Write the data to the file, describing the types of relationships within the ontology
    with open(ONTOLOGY_CONNECTIONS, mode='w+',newline='') as connectionsFile:
        relationshipWriter=csv.writer(connectionsFile,delimiter=',')
        relationshipWriter.writerow(["Number of intermediaries", "Relationships with X intermediaries"])
        for intermediaries in relationships.keys():
            relationshipWriter.writerow([intermediaries, relationships[intermediaries]])

def getOntologyNames():
    names = []
    with open(NAMES_FILE, mode='r') as nameFile:
        nameReader=csv.reader(nameFile,delimiter=',')
        for row in nameReader:
            if row[0] != "name":
                names.append(row[0])
    return names