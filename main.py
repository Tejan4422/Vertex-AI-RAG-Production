import functions_framework
import json
import requests
import hashlib
from google.cloud import firestore, bigquery, storage
from datetime import datetime
from sentence_transformers import SentenceTransformer
from google.auth.transport.requests import Request
from google.auth import default
import functions_framework
import pandas as pd  # Add pandas for reading XLSX files

# Initialize Sentence-BERT model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Initialize Firestore and BigQuery clients
db = firestore.Client()
client = bigquery.Client(project='')  # Replace with your project ID
dataset_id = ''
table_id = f'{dataset_id}.rfp_queries_responses_timestamps'

# Initialize Cloud Storage client
storage_client = storage.Client()

# Function to get access token for authentication
def get_access_token():
    credentials, _ = default()
    credentials.refresh(Request())
    return credentials.token

# Function to call the Discovery Engine API for a single query
def call_discovery_engine(query_text, session_id=None):
    url = ""
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "query": {
            "text": query_text
        },
        "session": session_id or "",  # Pass empty session_id for the first query
        "answerGenerationSpec": {
            "ignoreAdversarialQuery": True,
            "ignoreNonAnswerSeekingQuery": True,
            "ignoreLowRelevantContent": True,
            "includeCitations": True,
            "promptSpec": {
                "preamble": (
                    "Please keep the answer concise and limit it to between 100 to 200 words. "
                    "Treat the input as a question and provide a summary. "
                    "Do not refer to the document names, banks, or entities directly. "
                    "Provide and construct summary in a way where the user should feel they are retrieving answers in a chat format."
                )
            },
            "modelSpec": {
                "modelVersion": "preview"  # Use the required model version
            }
        }
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # Check if the request was successful
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Request failed: {e.response.status_code} - {e.response.text}")
        return None

# Function to generate a valid document ID using SHA256
def generate_document_id(query_text):
    return hashlib.sha256(query_text.encode('utf-8')).hexdigest()

# Function to get a query response from Firestore
def get_query_from_firestore(query_text):
    doc_id = generate_document_id(query_text)
    doc_ref = db.collection('query_cache').document(doc_id)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()['response']
    return None

# Function to store the query and response in Firestore
def store_query_in_firestore(query_text, response_text):
  
    doc_id = generate_document_id(query_text)
    query_embedding = model.encode([query_text])[0]  # Generate embedding for the query

    # Reference to Firestore document
    doc_ref = db.collection('query_cache').document(doc_id)
    doc_ref.set({
        'query': query_text,
        'response': response_text,
        'timestamp': firestore.SERVER_TIMESTAMP,  # Automatically set timestamp
        'embedding': query_embedding.tolist()  # Store the embedding as a list
    })
    print(f"Stored query '{query_text}' and response in Firestore.")

def store_query_response(query, response, session_id):

    timestamp = datetime.utcnow().isoformat()
    rows_to_insert = [{"query": query, "response": response, "session_id": session_id, "timestamp": timestamp}]
    errors = client.insert_rows_json(table_id, rows_to_insert)  # API request
    if errors == []:
        print("Query and response stored successfully in BigQuery.")
    else:
        print("Error storing data in BigQuery:", errors)

@functions_framework.cloud_event
def hello_gcs(cloud_event):


    """Handles incoming Cloud Storage events."""
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']

    # Get the file content from Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_bytes()
    # Process each sheet in the XLSX file
   xls = pd.ExcelFile(file_content)  

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)  
        print(f"Processing sheet: {sheet_name}")

        for index, row in df.iterrows():
            query_text = row.get('requirements') or row.get('Bank Requirements')
            if not query_text:
                print(f"No query provided in row {index} of sheet {sheet_name}!")
                continue

            print(f"Processing Query: {query_text}")

            # Check Firestore for similar query
            cached_response = get_query_from_firestore(query_text)
            if cached_response:
                # If query exists in Firestore, use cached response
                print(f"Using cached response from Firestore: {cached_response}")
                response_text = cached_response
            else:
                # If query is not in cache, call the Discovery Engine API
                print("Query not found in cache, calling Discovery Engine API...")
                session_id = None  # Initialize session_id if required
                response_json = call_discovery_engine(query_text, session_id)

                if response_json:
                    response_text = response_json.get("answer", {}).get("answerText", "")
                    print(f"Response: {response_text}")

                    # Store the query, response, and session ID in BigQuery
                    session_id = response_json.get("session", session_id)  # Capture the session_id from the response
                    store_query_response(query_text, response_text, session_id)

                    # Store the new query and response in Firestore
                    store_query_in_firestore(query_text, response_text)
                else:
                    response_text = 'Error: No response received from the Discovery Engine API'

            # Store the response in the DataFrame
            df.at[index, 'Response'] = response_text

        # Save the updated DataFrame back to the same bucket with a new file name
        processed_file_name = f'processed_{file_name}'
        processed_blob = bucket.blob(processed_file_name)

        # Save as XLSX
        with processed_blob.open('wb') as f:
            df.to_excel(f, index=False, engine='openpyxl')
        print(f"Processed file saved as {processed_file_name}")

