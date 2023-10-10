import pymongo


class Connector(object):

    def __init__(self,client_name,target="local") -> None:

        self.client_name = client_name
        self.target = target

        if target == "local":
            self.__local_connect(self.client_name)
        elif target == "remote":
            self.__remote_connect(self.client_name)

    def __local_connect(self,client_name):
        """
        Local connection to  a specific client
        """
        mongo_host = 'localhost'  # The hostname of your MongoDB server
        mongo_port = 27017  # The default MongoDB port

        # MongoDB connection URI
        connection_uri = f"mongodb://{mongo_host}:{mongo_port}/{client_name}"

        # Connect to MongoDB
        try:
            # client = pymongo.MongoClient(connection_uri, username=username, password=password)
            self.client = pymongo.MongoClient(connection_uri)
            
            self.database = self.client[client_name]
            print(f"Connected to {self.target}/{self.client_name} successfully!")

        except pymongo.errors.ConnectionFailure as e:
            print("Failed to connect to MongoDB:", e)

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

        return schema_details
    
    def get_schema_id(self,schema_name):
        """
        Returns the schema id for a given schema name
        """
        collection = self.database.get_collection("schemas")
        schema_id = collection.find_one({"name":schema_name})['id']
        return schema_id
    
    def get_schema_details(self,schema_name):
        """
        Returns the schema details for a given schema name
        """
        collection = self.database.get_collection("schemas")
        schema_details = collection.find_one({"name":schema_name})
        return schema_details

    def __remote_connect(self):
        """
        Remote connection to a specific client
        """
        print("Remote connection not implemented yet")
        self.client = None
        self.database = None
    
    def disconnect(self):
        self.client.close()
        print(f"Disconnected from {self.target}/{self.client_name} successfully!")


def report_schemas(schema_list='not passed'):
    """
    Reports the schema name and id for requested schemas
    """

    sorted_schemas = sorted(mongo_conn.all_schemas(), key=lambda k: k['name'])
    sorted_names = [i['name'] for i in sorted_schemas] # used to check if schema is available

    if schema_list=='not passed':
        print(f'\nThe full {len(sorted_schemas)} schema(s) are:\n')
        filtered = sorted_schemas
    else:
        if type(schema_list) != list:
            print("Expected a list of schema names")
            raise TypeError
        else:

            missing_items = [item for item in schema_list if item not in sorted_names]
            if len(missing_items) > 0:
                print(f"The following schema(s) are not available in that instance: {missing_items}")
                sys.exit(1)
            
            filtered = [i for i in sorted_schemas if i['name'] in schema_list]

    for counter, i in enumerate(filtered,1):

            file = i['name']
            schema_id = i['id']
            print(f"{counter}: {file} = {schema_id}")


if __name__ == "__main__":

    mongo_conn = Connector("xefr-signify-dev")

    report_schemas(['TSP UK Placements Forecast'])
    print(mongo_conn.get_schema_id('TSP UK Placements Forecast'))
    print(mongo_conn.get_schema_details('TSP UK Placements Forecast'))

    mongo_conn.disconnect()
    