from langchain_core.prompts import PromptTemplate
from langchain_neo4j import Neo4jGraph
from huggingface_hub import InferenceClient
from neo4j.exceptions import CypherSyntaxError
import yaml
import os
import ast


class MovieChatBot():
    
    #Initialization of the chatbot
    #Arguments:
    # hf_token:str - HuggingFace API token for LLM
    # db_connection:dict[str,str] - Dictionary with Neo4j connection parameters (url, username, password, database)
    # model:str - model name for LLM
    # max_tokens:int - Maximum tokens allowed for LLM response
    #Returns: None
    #Raises: KeyError if required keys are missing in db_connection
    def __init__(self, hf_token: str, db_connection: dict[str, str], model:str, max_tokens:int):
        
        #State variables
        self._chat_history = []    #Stores conversation history
        self._rag_question = ""    #Current RAG question
        self._question = ""        #Current user question
        self._answer = ""          #Current answer from LLM
        self._context = []         #Additional context for RAG
        self._previous_queries = [] #List of previous failed Cypher queries
        self._used_queries = []     #List of used queries that returned context
        
        #LLM client
        self._client = InferenceClient(model=model, token=hf_token)
        self._max_tokens = max_tokens
        
        #Prompt templates
        base_path = os.path.dirname(__file__)
        prompts_path = os.path.join(base_path, "prompts.yaml")
        with open(prompts_path, "r", encoding="utf-8") as f:
            prompts = yaml.safe_load(f)
        self._cypher_prompt_template = PromptTemplate.from_template(
            prompts["cypher_prompt"], 
            partial_variables={"cypher_schema": prompts["cypher_schema"], "cypher_examples": prompts["cypher_examples"]}
        )
        self._base_template = PromptTemplate.from_template(prompts["base"])
        
        #Neo4j graph database connection
        missing_keys = list(set(["url", "username", "password", "database"]) - set(db_connection.keys()))
        if len(missing_keys) > 0:
            raise KeyError(f"Missing keys in db_connection {missing_keys}")
        else:
            self._graphDB = Neo4jGraph(
                url = db_connection["url"],
                username = db_connection["username"],
                password = db_connection["password"],
                database = db_connection["database"]
            )
    
    ### SET OF FUNCTIONS TO CREATE MODEL PROMPT AND GET ANSWER
    
    #Generate answer from LLM given system and user prompts
    #Arguments:
    # system_prompt:str - System message to guide the LLM
    # user_prompt:str - User question or input
    #Returns: str - Generated response from LLM
    def generate_chat_anserw(self, system_prompt:str, user_prompt:str):
        messages = [{"role": "system", "content": system_prompt}]
        messages += [{"role": "user", "content": user_prompt}]
        response = self._client.chat_completion(
            messages=messages,
            max_tokens = self._max_tokens,
            temperature = 0.1
        )
        return response.choices[0].message["content"]
    
    #Generate a Cypher query using LLM based on current RAG question
    #Arguments: None
    #Returns: str - Generated Cypher query or list of queries
    def generate_cypher_query(self):
        return self.generate_chat_anserw(
            system_prompt = "You are an expert Neo4j Cypher generator.",
            user_prompt = self._cypher_prompt_template.invoke({
                "question": self._rag_question, 
                "previous_queries": self._previous_queries,
                "used_quries": self._used_queries
            }).to_string()
        )
    
    #Save current conversation to chat history
    #Arguments: None
    #Returns: None
    def save_chat_history(self):
        self._chat_history.append({"role": "user", "content": self._question, "context": self._context})
        self._chat_history.append({"role": "assistant", "content": self._answer})
        
    #Restart the conversation and reset all state variables
    #Arguments: None
    #Returns: None
    def restart(self):
        self._chat_history = []
        self._rag_question = ""
        self._question = ""
        self._answer = ""
        self._context = []
        self._previous_queries = []
        self._used_queries = []
    
    ### MAIN APP
    
    #Main interactive chat loop
    #Arguments: None
    #Returns: None
    def chat(self):
        print("Chatbot ready! Type 'exit' to quit. Type 'restart' to restart whole conversation.\n")
        while True:
            #Getting question from user
            user_q = input("You: ")
            
            #End of app
            if user_q.lower() in ["exit", "quit"]:
                self.restart()
                break
            
            #Restart conversation
            if user_q.lower() == "restart":
                self.restart()
                os.system("cls" if os.name == "nt" else "clear")
                print("Chatbot ready! Type 'exit' to quit. Type 'restart' to restart whole conversation.\n")
            else:
                #Conversation with LLM
                self._question = user_q
                self._context = []
                self._used_queries = []
                self._previous_queries = []
                
                #Generate user prompt using base template
                user_prompt = self._base_template.invoke({
                    "question": self._question,
                    "context" : self._context,
                    "chat_history": self._chat_history
                }).to_string()
                
                #Get initial answer from LLM
                self._answer = self.generate_chat_anserw(
                    system_prompt = "You are movie chatbot",
                    user_prompt = user_prompt
                )
                
                #If answer contains NO_CONTEXT, use RAG to retrieve additional context
                while "NO_CONTEXT" in self._answer:
                    rag_question = self._answer.split("NO_CONTEXT")
                    self._rag_question = rag_question[-1].strip()
                    cypher_queries_list = self.generate_cypher_query()
                    
                    #Transform string to list
                    try:
                        cypher_queries_list = ast.literal_eval(cypher_queries_list)
                    except SyntaxError:
                        cypher_queries_list = [cypher_queries_list]

                    #Execute each generated query
                    for i, cypher_query in enumerate(cypher_queries_list):
                        try:
                            
                            context = str(self._graphDB.query(cypher_query))
                            if context.strip() == "[]":
                                cypher_query = "No data generated for this query: " + cypher_query
                                context = None
                        except CypherSyntaxError:
                            print("System: System error, cannot provide additional data\n")
                            cypher_query = "Invalid syntax of this query: " + cypher_query
                            context = None
                        
                        #Save failed queries
                        if context == None:
                            self._previous_queries.append(cypher_query)
                        else:
                            self._context.append(context)
                            self._used_queries.append(cypher_query)

                    #Generate answer again with additional context
                    user_prompt = self._base_template.invoke({
                        "question": self._question,
                        "context":  self._context,
                        "chat_history": self._chat_history
                    }).to_string()
                    
                    self._answer = self.generate_chat_anserw(
                        system_prompt = "You are movie chatbot",
                        user_prompt = user_prompt,
                    )

                #Print answer and save to chat history
                print(f"Bot: {self._answer}\n")
                self.save_chat_history()

