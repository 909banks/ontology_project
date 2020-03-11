import os
import sys
import socket, errno
from SPARQLWrapper import SPARQLWrapper, CSV

class Interface:
    def __init__(self, url):
        # Default values for the graphURL and default port, these values are provided
        # by the top-level python file search.py
        self.graphURL=url
        self.sparql=SPARQLWrapper(url)
        self.defaultPort=7200

    def startGraphDB(self, graphExecutable):
        """
        Method used to run the graphDB executable if it is installed, else
        it will raise an exception and tell the user to install graphDBFree
        """

        # Need to remove any extra quotation marks in the string necessary for the
        # os.system execute, to be accessable for the os.path commands
        filePath=graphExecutable.replace('"','')
        if not os.path.exists(filePath) or not os.access(filePath, os.X_OK):
            print(os.path.exists(filePath))
            print(os.access(filePath, os.X_OK))
            raise OSError ("GraphDB not found at %s, please install and re-run this program" % (graphExecutable))
        os.system(graphExecutable)

    def connectToGraph(self, graphExecutable, graphURL):
        """
        This method is used to connect to the ontology onstart-up if necessary, 
        it will ensure GraphDB is running and has a stable connection for us to 
        query the ontology with
        """
        graphRunning=False
        socketOpen=False
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host=socket.gethostname()

        # Strat GraphDB and connect to the default port with the correct repository
        # We have assumed that the repository is already connected and set as the 
        # default repository
        while socketOpen==False:

            # Attempt to open the port, if GraphDB is already open it will be unable 
            # to bind and throw an error, if the port is free we can query the ontology
            try:
                s.bind((host, self.defaultPort))
                s.close()

            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    graphRunning=True
                    socketOpen=True

                elif graphRunning==False:
                    print("Starting GraphDB connection")
                    self.startGraphDB(graphExecutable)
                    graphRunning=True

                else:
                    print("No ontology found")
                    sys.exit()
        
        self.graphURL=graphURL
        self.sparql=SPARQLWrapper(self.graphURL)
        return True


    def queryOntology(self, name=str):
        """Method used to query the selected ontology using the python SPARQL interface
        
        Keyword Arguments:
            name {string} -- The name of the person we wish to find all connections to in the ontology (default: {str})
        
        Returns:
            [[str, str]] -- List of the names and comapny they work for that are connected to the initial name given
        """

        self.sparql.setQuery('''
            PREFIX rdfs: <http://www.ontotext.com/explicit>
            SELECT
            WHERE
        ''')

        self.sparql.setReturnFormat(CSV)
        results = self.sparql.query().convert()
        print(results)

        return results


    def processQueryResults(self, results=[str]):
        pass
