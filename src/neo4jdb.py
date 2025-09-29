from neo4j import GraphDatabase
import pandas as pd
from langchain_huggingface import HuggingFaceEmbeddings

#Class for Neo4j database with information about movies and their related data
class Neo4jDB:
    
    #Initialization of the class, creates a connection to the database
    #Arguments:
    # uri:str - Neo4j connection URI
    # user:str - Username for Neo4j
    # password:str - Password for Neo4j
    # db_name:str - Database name to connect
    # files_names:dict - Dictionary with file names for loading data (movies, actors, genres, etc.)
    #Returns: None
    def __init__(self, uri:str, user:str, password:str, db_name:str, files_names:dict):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._db_name = db_name
        self._files_names = files_names
    
    #Destructor of the class, closes the connection
    #Arguments: None
    #Returns: None
    def __del__(self):
        self._driver.close()

    #Function to execute a query in the database
    #Arguments:
    # query:str - Cypher query string
    # parameters:dict - Optional dictionary of parameters for the query
    #Returns: List of dictionaries with query results
    def execute_query(self, query:str, parameters:dict = None):
        with self._driver.session(database=self._db_name) as session:
            return session.run(query, parameters or {}).data()
    
    #Function to check if constraints exist and create them if not
    #Arguments:
    # label:str - Node label
    # property:str - Property for which the constraint should be applied
    #Returns: None
    def check_constraints(self, label:str, property:str):
        query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property} IS UNIQUE"
        self.execute_query(query, parameters={'label': label, 'property': property})
        
    #Function to create index if it does not exist
    #Arguments:
    # label:str - Node label
    # property:str - Property for which the index should be created
    #Returns: None
    def create_index(self, label:str, property:str):
        query = f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{property})"
        self.execute_query(query, parameters={'label': label, 'property': property})
    
    #Create movie nodes with properties
    #Arguments: None
    #Returns: None
    #Raises: ValueError if movies file not provided
    def create_movie(self):
        if("movies" not in self._files_names):
            raise ValueError("Movies file name not provided in files_names dictionary")
        else:
            self.check_constraints("Movie", "id")
            print(f"Loading movies data from ${self._files_names['movies']} file...")
            movies_df = pd.read_csv(self._files_names['movies'])
            if pd.api.types.is_datetime64_any_dtype(movies_df['release_date']):
                movies_df['release_date'] = movies_df['release_date'].dt.strftime("%Y-%m-%d")
            movies_data = movies_df.to_dict('records')
            for row in movies_data:
                for key, value in row.items():
                    if isinstance(value, float) and pd.isna(value):
                        row[key] = None
            query = """
            UNWIND $movies AS movie
            MERGE (m:Movie {id: movie.id})
            ON CREATE SET 
                m.title          = movie.title,
                m.original_title = movie.original_title,
                m.overview       = movie.overview,
                m.release_date   = CASE 
                                    WHEN movie.release_date IS NOT NULL AND movie.release_date <> '' 
                                    THEN date(movie.release_date) 
                                    ELSE NULL 
                                    END,
                m.budget         = movie.budget,
                m.popularity     = movie.popularity,
                m.revenue        = movie.revenue,
                m.runtime        = movie.runtime,
                m.vote_average   = movie.vote_average,
                m.vote_count     = movie.vote_count
            """
            self.execute_query(query, parameters={'movies': movies_data})
            print("Movies data loaded successfully.")
    
    #Base function to create nodes and relationships for movies
    #Arguments:
    # file_key:str - Key in files_names dictionary for the file
    # node_label:str - Label of the node to be created
    # node_id_column:str - Column in CSV used as node id
    # movie_id_column:str - Column in CSV containing related movie id
    # node_properties:dict - Mapping of node property names to CSV columns
    # rel_label:str - Label of the relationship to the movie
    # additional_labels:list - Optional additional labels for the node
    # rel_properties:dict - Optional properties for the relationship
    #Returns: None
    #Raises: ValueError if file_key not in files_names
    def add_nodes_base_function(self, file_key:str, node_label:str, node_id_column:str, movie_id_column:str,
                                node_properties:dict, rel_label:str, additional_labels:list = None, rel_properties:dict = None):
        if (file_key not in self._files_names):
            raise ValueError(f"{file_key} file name not provided in files_names dictionary")
        else:
            self.check_constraints(node_label, node_id_column)
            print(f"Loading {file_key} data from ${self._files_names[file_key]} file...")
            df = pd.read_csv(self._files_names[file_key])
            data = df.to_dict('records')
            for row in data:
                for key, value in row.items():
                    if isinstance(value, float) and pd.isna(value):
                        row[key] = None
            query = f"""
            UNWIND $data AS row
            MERGE (n:{node_label} {{{node_id_column}: row.{node_properties[node_id_column]}}})
            """
            if len(node_properties) > 1 or additional_labels:
                query += "ON CREATE SET\n"
            if len(node_properties) > 1:
                for k, v in node_properties.items():
                    if k != node_id_column:
                        query += f"    n.{k} = row.{v},\n"
                if not additional_labels:
                    query = query.rstrip(',\n') + "\n"
            if additional_labels:
                for al in additional_labels:
                    query += f"    n:{al},\n"
                query = query.rstrip(',\n') + "\n"
                query += "ON MATCH SET\n"
                for al in additional_labels:
                    query += f"    n:{al},\n"
                query = query.rstrip(',\n') + "\n"
            query += f"""WITH n, row
            MATCH (m:Movie {{id: row.{movie_id_column}}})
            MERGE (n)-[r:{rel_label}]->(m)
            """
            if rel_properties:
                query += "SET\n"
                for k, v in rel_properties.items():
                    query += f"    r.{k} = row.{v},\n"
                query = query.rstrip(',\n') + "\n"
            batch_size = 10000
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                self.execute_query(query, parameters={'data': batch})
            print(f"{file_key.capitalize()} data loaded successfully.")
    
    #Create actors nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_actors(self):
        self.add_nodes_base_function(
            file_key = "actors",
            node_label = "Person",
            node_id_column = "person_id",
            movie_id_column = "movie_id",
            node_properties = {'person_id': 'person_id', 'name': 'person_name'},
            rel_label = "ACTED_IN",
            additional_labels = ['Actor'],
            rel_properties = {'character': 'character'}
        )
        
    #Create directors nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_directors(self):
        self.add_nodes_base_function(
            file_key = "directors",
            node_label = "Person",
            node_id_column = "person_id",
            movie_id_column = "movie_id",
            node_properties = {'person_id': 'person_id', 'name': 'person_name'},
            rel_label = "DIRECTED",
            additional_labels = ['Director'],
            rel_properties = None
        )
    
    #Create crew nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_crew(self):
        self.add_nodes_base_function(
            file_key = "crew",
            node_label = "Person",
            node_id_column = "person_id",
            movie_id_column = "movie_id",
            node_properties = {'person_id': 'person_id', 'name': 'person_name'},
            rel_label = "WORKED_AS",
            additional_labels = ['Crew'],
            rel_properties = {'department': 'department', 'job': 'job'}
        )

    #Create genres nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_genres(self):
        self.add_nodes_base_function(
            file_key = "genres",
            node_label = "Genre",
            node_id_column = "genre_id",
            movie_id_column = "movie_id",
            node_properties = {'genre_id': 'genre_id', 'name': 'genre_name'},
            rel_label = "OF_GENRE",
            additional_labels = None,
            rel_properties = None
        )

    #Create keywords nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_keywords(self):
        self.add_nodes_base_function(
            file_key = "keywords",
            node_label = "Keyword",
            node_id_column = "keyword_id",
            movie_id_column = "movie_id",
            node_properties = {'keyword_id': 'keyword_id', 'name': 'keyword_name'},
            rel_label = "HAS_KEYWORD",
            additional_labels = None,
            rel_properties = None
        )

    #Create collections nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_collections(self):
        self.add_nodes_base_function(
            file_key = "collections",
            node_label = "Collection",
            node_id_column = "collection_id",
            movie_id_column = "movie_id",
            node_properties = {'collection_id': 'collection_id', 'name': 'collection_name'},
            rel_label = "PART_OF_COLLECTION",
            additional_labels = None,
            rel_properties = None
        )

    #Create production companies nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_production_companies(self):
        self.add_nodes_base_function(
            file_key = "production_companies",
            node_label = "ProductionCompany",
            node_id_column = "company_id",
            movie_id_column = "movie_id",
            node_properties = {'company_id': 'prod_comp_id', 'name': 'prod_comp_name'},
            rel_label = "PRODUCED_BY",
            additional_labels = None,
            rel_properties = None
        )

    #Create production countries nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_production_countries(self):
        self.add_nodes_base_function(
            file_key = "production_countries",
            node_label = "ProductionCountry",
            node_id_column = "country_code",
            movie_id_column = "movie_id",
            node_properties = {'country_code': 'prod_coun_code', 'name': 'prod_coun_name'},
            rel_label = "PRODUCED_IN",
            additional_labels = None,
            rel_properties = None
        )

    #Create spoken languages nodes with properties and relationships with movies
    #Arguments: None
    #Returns: None
    def create_spoken_languages(self):
        self.add_nodes_base_function(
            file_key = "spoken_languages",
            node_label = "SpokenLanguage",
            node_id_column = "language_code",
            movie_id_column = "movie_id",
            node_properties = {'language_code': 'lang_code', 'name': 'lang_name'},
            rel_label = "SPOKEN_IN",
            additional_labels = None,
            rel_properties = None
        )

    #Create embeddings for nodes using HuggingFaceEmbeddings
    #Arguments:
    # node_label:str - Node label
    # property:str - Property of node to create embedding for
    # embedding_model:HuggingFaceEmbeddings - Embedding model
    # update:bool - If True, only nodes without embedding will be updated
    #Returns: None
    def create_embeddings(self, node_label:str, property:str, embedding_model:HuggingFaceEmbeddings = None, update:bool = False):
        if update:
            update_str = f"AND n.{property}_embedding IS NULL"
        else:
            update_str = ""
        query = f"""
        MATCH (n:{node_label})
        WHERE n.{property} IS NOT NULL {update_str}
        RETURN n.id AS id, n.{property} AS {property}
        """
        result = self.execute_query(query, parameters=None)
        query = f"""
        UNWIND $data AS row
        MATCH (n:{node_label} {{id: row.id}})
        CALL db.create.setNodeVectorProperty(n, '{property}_embedding', row.embedding);
        """
        batch_size = 100
        for i in range(0, len(result), batch_size):
            batch = [{'id': row['id'], 'embedding': embedding_model.embed_query(row[property])} 
                     for row in result[i:i+batch_size]]
            self.execute_query(query, parameters={'data': batch})
            print(f"Number of updated properties = {batch_size+i}")
        print("Embeddings created successfully")

    #Create vector index for a node property
    #Arguments:
    # node_label:str - Node label
    # node_prop:str - Node property to create vector index on
    # index_name:str - Name of the index
    # vector_dimension:int - Dimension of embedding vector
    #Returns: None
    def create_embedding_index(self, node_label:str, node_prop:str, index_name:str, vector_dimension:int):
        query = f"""
        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
        FOR (n:{node_label})
        ON n.{node_prop}
        OPTIONS {{indexConfig: {{
        `vector.dimensions`: {vector_dimension},
        `vector.similarity_function`: 'cosine'
        }}}}
        """
        self.execute_query(query, parameters=None)

    #Create info property for node based on its properties and neighbours
    #Arguments:
    # node_label:str - Node label
    # new_prop_name:str - Name of the new property
    # embedding_model:HuggingFaceEmbeddings - Embedding model
    # update:bool - If True, only nodes without info will be updated
    #Returns: None
    def create_node_info(self, node_label:str, new_prop_name:str, embedding_model:HuggingFaceEmbeddings = None, update:bool = False):
        if update:
            update_str = f"WHERE n.{new_prop_name} IS NULL \n"
        else:
            update_str = ""
        query = f"""
        MATCH (n:{node_label})
        {update_str} RETURN n.id AS id
        """
        movie_list = self.execute_query(query, parameters=None)
        movie_ids = [row['id'] for row in movie_list]
        print(f"{len(movie_ids)} movies to process")
        batch_size = 10
        for i in range(0, len(movie_ids), batch_size):
            query = f"""
            MATCH (n:{node_label})
            WHERE n.id IN $id_list
            OPTIONAL MATCH (n)-[r]-(m)
            RETURN n, collect({{relType:type(r), relProps:properties(r), neighbour:properties(m)}}) AS relData
            """
            movies_info = self.execute_query(query, parameters={'id_list': movie_ids[i:i+batch_size]})
            movies_info_restructures = []
            for movie in movies_info:
                movie_node = movie['n']
                movie_id = movie_node['id']
                movie_node_str = "Movie Info "
                for prop, value in movie_node.items():
                    if 'id' not in prop and 'embedding' not in prop:
                        movie_node_str += (prop + ": " + str(value) + ", ")
                movie_node_str = movie_node_str.rstrip(", ") + " "
                neighbours = movie['relData']
                neighbours_str = "|| ADDITIONAL DATA | "
                for neighbour in neighbours:
                    neighbour_str = str(neighbour['relType']) + " "
                    neighbour_props = neighbour['neighbour']
                    if neighbour_props:
                        for prop, value in neighbour_props.items():
                            if 'id' not in prop and 'embedding' not in prop:
                                neighbour_str += ((prop + ": " + str(value) + ", "))
                    rel_props = neighbour['relProps']
                    if rel_props:
                        for prop, value in rel_props.items():
                            if 'id' not in prop and 'embedding' not in prop:
                                neighbour_str += ((prop + ": " + str(value) + ", "))
                    neighbour_str = neighbour_str.rstrip(", ") + " | "
                    neighbours_str += neighbour_str
                neighbours_str = neighbours_str.rstrip("| ") + "||"
                movie_info = movie_node_str+neighbours_str
                movies_info_restructures.append({'id': movie_id, 'movie_info': movie_info, 'embedding': embedding_model.embed_query(movie_info) })
            query = f""" 
            UNWIND $data AS row
            MATCH (n:{node_label} {{id: row.id}})
            SET n.{new_prop_name} = row.movie_info,
                n.{new_prop_name}_embedding = row.embedding
            """
            self.execute_query(query, parameters={'data': movies_info_restructures})
            print(f"Number of updated properties = {batch_size+i}")

    #Create all nodes, relationships, indexes and constraints in the database
    #Arguments: None
    #Returns: None
    def create_db(self):
        self.create_movie()
        print("\n")
        self.create_index("Movie", "id")
        print("Index on movie id created successfully.")
        print("\n")
        self.create_index("Movie", "title")
        print("Index on movie title created successfully.")
        print("\n")
        self.create_actors()
        print("\n")
        self.create_directors()
        print("\n")
        self.create_crew()
        print("\n")
        self.create_index("Person", "name")
        print("Index on person name created successfully.")
        print("\n")
        self.create_genres()
        print("\n")
        self.create_keywords()
        print("\n")
        self.create_collections()
        print("\n")
        self.create_production_companies()
        print("\n")
        self.create_production_countries()
        print("\n")
        self.create_spoken_languages()
        print("\n Data import completed successfully.")




