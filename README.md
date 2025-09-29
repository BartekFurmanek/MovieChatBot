# Movie Chatbot with Neo4j and HuggingFace
This project is a **movie chatbot** powered by **LLMs (HuggingFace)** and a **Neo4j graph database**.  
The app enables interactive conversations about movies using data from [CMU Personas Dataset](https://www.cs.cmu.edu/~ark/personas/).  

##  Project Structure
- .env # environment variables
- requirements.txt # required libraries
- install.ps1 # script for creating Python environment and installing dependencies
- start_app.ps1 # script to run the app
│
- data
│ - raw/ # raw data (downloaded from CMU site)
│ - clean/ # processed data ready for Neo4j
│
-  notebooks
│ - data_prep/ # notebooks for data preprocessing
│
- src
│ - prompts.yaml # LLM prompt templates
│ - chatbot.py # chatbot logic
│ - neo4jdb.py # Neo4j database class
│ - db.py # database initialization script
│ - app.py # main chatbot application

## Requirements
- **Python 3.10+**
- **Neo4j** (local or cloud)
- HuggingFace account + API token
- Windows OS (PowerShell `.ps1` scripts)

## Key libraries:
- `langchain`
- `langchain-neo4j`
- `langchain-huggingface`
- `huggingface-hub`
- `neo4j`
- `pandas`
- `dotenv`

(See details in `requirements.txt`)

## Environment Variables
Put them in your `.env` file:

- HF_TOKEN=your_huggingface_token
- MODEL=mistralai/Mistral-Small-3.1-24B-Instruct-2503
- MAX_TOKENS=10000
- URI=bolt://localhost:7687
- USER=neo4j
- PASSWORD=your_neo4j_password
- DB_NAME=movies

## Installation & Running

### 1. Configure Neo4j
- Set up a local or cloud Neo4j instance.  
- Copy connection details (**URI, user, password, db name**) to `.env`.

### 2. Run installation
- Run in PowerShell ./install.ps1
- This will create a virtual Python environment, install all dependencies, and initialize the Neo4j database with data from data/clean.

### 3. Run the app
- Run in PowerShell ./start_app.ps1
- The chatbot will start in interactive mode.

## Features
- Data import into **Neo4j** with nodes, relationships, indexes, and embeddings  
- Conversational interface with an LLM  
- **RAG (Retrieval-Augmented Generation)**: generates **Cypher queries** when additional context is needed  
- Stores chat history and supports restart  

## Notebooks
Located in `notebooks/data_prep/`, they handle:
- preprocessing raw data (`raw` → `clean`),  
- preparing CSV files for Neo4j import.  


## Database
Created nodes & relationships include:
- Movies (`Movie`)  
- Actors (`Actor`), Directors (`Director`), Crew (`Crew`)  
- Genres (`Genre`), Keywords (`Keyword`)  
- Collections (`Collection`)  
- Production Companies (`ProductionCompany`), Countries (`ProductionCountry`)  
- Spoken Languages (`SpokenLanguage`)  

## Important: Model Requirements

To make this chatbot work, you must select a model that is available through **HuggingFace Inference Providers**.  
The chosen model must support **conversational mode**, i.e., recognize and correctly handle roles:  
`user`, `system`, and `assistant`.  

This chatbot was developed and tested with:  
**`mistralai/Mistral-Small-3.1-24B-Instruct-2503`**.  


If you use a different model, additional **prompt engineering** in `prompts.yaml` may be required to achieve correct results.
