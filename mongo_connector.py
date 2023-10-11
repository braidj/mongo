import pymongo
import sys

class Mongo(object):


    def __init__(self,target,database,logger) -> None:

        self.database = database
        self.target = target
        self.logger = logger

        if target == "LOCAL":
            self.__local_connect(self.database)
        elif target == "REMOTE":
            self.__remote_connect(self.database)

        logger.info(f"Mongo: initialised for {target} instance on {database} using {self.connection_uri}")

    def __local_connect(self,database):
        """
        Local connection to  a specific client
        """
        mongo_host = 'localhost'  # The hostname of your MongoDB server
        mongo_port = 27017  # The default MongoDB port

        # MongoDB connection URI
        self.connection_uri = f"mongodb://{mongo_host}:{mongo_port}/{database}"

        # Connect to MongoDB
        try:
            # client = pymongo.MongoClient(connection_uri, username=username, password=password)
            self.client = pymongo.MongoClient(self.connection_uri)
            
            self.database = self.client[database]

        except pymongo.errors.ConnectionFailure as e:
            self.logger.error(f"{Mongo.__name__} Failed to connect to MongoDB: {e}")
            print("Failed to connect to MongoDB:", e)
            sys.exit(1)

    def all_schemas(self):
        """
        Returns key details of the schemas
        """
        schema_details = []
        collection = self.database.get_collection("schemas")
        documents = collection.find()
        for doc in documents:
            row = {'id':doc['id'],'name':doc['name'],'description':doc['description']}
            schema_details.append(row)

        self.logger.info(f"{Mongo.__name__}: {len(schema_details)} schemas found in {self.database}")

        return schema_details
    
    def get_schema_id(self,schema_name):
        """
        Returns the schema id for a given schema name
        """
        self.logger.info(f"{Mongo.__name__}: Getting schema id for {schema_name}")
        collection = self.database.get_collection("schemas")
        try:
            schema_id = collection.find_one({"name":schema_name})['id']
            self.logger.info(f"{Mongo.__name__}: Schema id for {schema_name} is {schema_id}")
            return schema_id
        
        except TypeError as e:

            if "'NoneType' object is not subscriptable" in str(e):
                # Handle the specific TypeError here
                print(f"Schema {schema_name} does not exist in {self.database}")
                self.logger.warning(f"{Mongo.__name__}: Schema {schema_name} does not exist in {self.database}")
                return 0
            else:
                # Handle other TypeErrors
                print("Caught a different TypeError:", e)
                self.logger.error(f"{Mongo.__name__}: Caught a different TypeError: {e}")
                return 0
    
    def get_schema_details(self,schema_name):
        """
        Returns the schema details for a given schema name
        """
        collection = self.database.get_collection("schemas")
        schema_details = collection.find_one({"name":schema_name})

        if type(schema_details) == type(None):
            self.logger.warning(f"{Mongo.__name__}: Schema {schema_name} does not exist in {self.database}")
            return 0
        else:
            self.logger.info(f"{Mongo.__name__}: Returned schema details for {schema_name}")
            return schema_details

    def __remote_connect(self):
        """
        Remote connection to a specific client
        """
        self.logger.info(f"{Mongo.__name__}: Remote connection not implemented yet")
        self.client = None
        self.database = None
    
    def disconnect(self):
        self.client.close()
        self.logger.info(f"Mongo: Disconnected from {self.target} instance")

