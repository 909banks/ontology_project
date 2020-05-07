import os
import socket, errno
import csv
import bisect
from SPARQLWrapper import SPARQLWrapper, CSV
from string import Template

class Interface:
    def __init__(self, url):
        # Default values for the graphURL and default port, these values are provided
        # by the top-level python file search.py
        self.graphURL=url
        self.sparql=SPARQLWrapper(url)
        self.defaultPort=7200
        self.expandedCompanies=[]
        
    def resetExpandedCompanies(self):
        """This method must be called between every search
        """
        self.expandedCompanies=[]

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
                # Error number for denied access which happens if the socket is already in use by GraphDB
                if e.errno == 10013:
                    graphRunning=True
                    socketOpen=True

                elif graphRunning==False:
                    print("Starting GraphDB connection")
                    self.startGraphDB(graphExecutable)
                    graphRunning=True

                else:
                    print("No ontology found")
                    exit()
        
        self.graphURL=graphURL
        self.sparql=SPARQLWrapper(self.graphURL)
        return True

    def queryOntology(self, currentNode={str:str, str:str}):
        """Method used to query the selected ontology using the python SPARQL interface
        
        Keyword Arguments:
            name {dict {"name", "companyID"} } -- The name and company they work at of the person we wish to find all connections to in the ontology (default: {str})
        
        Returns:
            [[str, str]] -- List of the names and company they work for that are connected to the initial name given
        """
        self.sparql.setReturnFormat(CSV)
        results=[]
        # Get the companies that the current person works at
        # Excluding the parent company we found the from

        # Modify so that the filter is for all explored companies
        query = Template("""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX york: <http://york.ac.uk/>
        SELECT ?company ?companyID 
        WHERE { 
            ?person foaf:name "$name".
            ?person york:worksat ?company.
            ?company york:tradingsymbol ?companyID.
            FILTER (?companyID NOT IN ('$CID')).
        }
        """)
        self.sparql.setQuery(query.substitute(name=currentNode["name"], CID=currentNode["companyID"]))
        csvResults = self.sparql.queryAndConvert().decode().splitlines()
        x=csv.reader(csvResults, delimiter=',')
        companyResults=list(x)[1:]
        for company in companyResults:
            if company[0] in self.expandedCompanies:
                companyResults = list(filter(lambda a: a!=company, companyResults))
        
        # Get the names of the people that work at the same companies, excluding the current person
        for company in companyResults:
            bisect.insort(self.expandedCompanies, company[0])

            query = Template("""
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX york: <http://york.ac.uk/>
                SELECT ?name 
                WHERE { 
                    ?person york:worksat <$company>.
                    ?person foaf:name ?name.
                    MINUS {
                        ?person foaf:name "$name"
                    }.
                }
            """)
            self.sparql.setQuery(query.substitute(company=company[0], name=currentNode["name"]))
            nameResults = self.sparql.queryAndConvert().decode().splitlines()[1:]
            for name in nameResults:
                name=name.replace('"','')
                temp = {"name":name, "companyID": company[1]}
                results.append(temp)

        # Return a list of dictionaries [ {"name":people names, "companyID":parent company id}, ...]
        return results
