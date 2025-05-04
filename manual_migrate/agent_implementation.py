import json
import aiohttp
import logging
import os
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Any, List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
BACKEND_SERVER = os.getenv("SERVER_URL", "http://localhost:8000")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY") #, "your-api-key-here"
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "localpassword")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# Create FastAPI app
app = FastAPI(servers=[{"url": BACKEND_SERVER}])

def get_schema() -> str:
    """
    Get the database schema by querying the database structure.
    Returns a string representation of the schema.
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        
        # Create a cursor with dictionary-like results
        cur = conn.cursor()
        
        # Query to get all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        tables = cur.fetchall()
        schema_info = []
        
        # For each table, get its columns and constraints
        for table in tables:
            table_name = table[0]
            
            # Get column information
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            
            columns = cur.fetchall()
            column_definitions = []
            
            for column in columns:
                column_name, data_type, is_nullable = column
                nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                column_definitions.append(f"{column_name} {data_type} {nullable_str}")
            
            # Get primary keys
            cur.execute(f"""
                SELECT c.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name = '{table_name}'
            """)
            
            primary_keys = [pk[0] for pk in cur.fetchall()]
            
            # Get foreign keys
            cur.execute(f"""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '{table_name}'
            """)
            
            foreign_keys = []
            for fk in cur.fetchall():
                column_name, foreign_table, foreign_column = fk
                foreign_keys.append(f"FOREIGN KEY ({column_name}) REFERENCES {foreign_table}({foreign_column})")
            
            # Compile table definition
            table_def = f"CREATE TABLE {table_name} (\n"
            table_def += ",\n".join(f"    {col}" for col in column_definitions)
            
            if primary_keys:
                table_def += f",\n    PRIMARY KEY ({', '.join(primary_keys)})"
                
            if foreign_keys:
                table_def += ",\n" + ",\n".join(f"    {fk}" for fk in foreign_keys)
                
            table_def += "\n);"
            
            schema_info.append(table_def)
        
        # Close the connection
        cur.close()
        conn.close()
        
        # Return the complete schema
        return "\n\n".join(schema_info)
        
    except Exception as e:
        logger.error(f"Error getting database schema: {e}")
        return "Error: Could not retrieve database schema"

async def query(sql_query: str) -> List[Dict[str, Any]]:
    """
    Execute the given SQL query on the database and return the results.
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            cursor_factory=RealDictCursor  # Returns results as dictionaries
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Execute the query
        cur.execute(sql_query)
        
        # Get the results
        results = cur.fetchall()
        
        # Convert to list of dictionaries for JSON serialization
        result_list = []
        for row in results:
            # Convert any non-serializable objects to strings
            serializable_row = {}
            for key, value in dict(row).items():
                if isinstance(value, (int, float, str, bool, type(None))):
                    serializable_row[key] = value
                else:
                    serializable_row[key] = str(value)
            result_list.append(serializable_row)
        
        # Close the connection
        cur.close()
        conn.close()
        
        return result_list
        
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return [{"error": f"Error executing SQL query: {str(e)}"}]

async def human_query_to_sql(human_query: str) -> str:
    """
    Convert a natural language query to SQL using the DeepSeek API.
    """
    database_schema = get_schema()

    system_message = f"""
    Given the following schema, write a SQL query that retrieves the requested information. 
    Return the SQL query inside a JSON structure with the key "sql_query".
    <example>
    {{
        "sql_query": "SELECT * FROM users WHERE age > 18;",
        "original_query": "Show me all users older than 18 years old."
    }}
    </example>
    <schema>
    {database_schema}
    </schema>
    """
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": system_message},
            {"role": "user", "content": human_query}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DEEPSEEK_API_URL,
                headers=headers,
                json=payload
            ) as response:
                response.raise_for_status()
                response_data = await response.json()
                return response_data['choices'][0]['message']['content']
    except Exception as e:
        logger.error(f"Error calling DeepSeek API for SQL conversion: {e}")
        return json.dumps({
            "sql_query": "",
            "error": f"Failed to generate SQL: {str(e)}"
        })

async def build_answer(result: List[Dict[str, Any]], human_query: str) -> str:
    """
    Build a natural language answer from SQL results using the DeepSeek API.
    """
    system_message = f"""
    Given a user's question and the SQL rows response from the database from which the user wants to get the answer,
    write a natural language response to the user's question.
    <user_question> 
    {human_query}
    </user_question>
    <sql_response>
    {json.dumps(result, indent=2)} 
    </sql_response>
    """

    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": system_message},
            {"role": "user", "content": "Generate a helpful response based on this data."}
        ],
        "temperature": 0.7
    }

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DEEPSEEK_API_URL,
                headers=headers,
                json=payload
            ) as response:
                response.raise_for_status()
                response_data = await response.json()
                return response_data['choices'][0]['message']['content']
    except Exception as e:
        logger.error(f"Error calling DeepSeek API for natural language response: {e}")
        return f"Error generating natural language response: {str(e)}"

class PostHumanQueryPayload(BaseModel):
    human_query: str

@app.post(
    "/human_query",
    name="Human Query",
    operation_id="post_human_query",
    description="Gets a natural language query, internally transforms it to a SQL query, queries the database, and returns the result.",
)
async def human_query_endpoint(payload: PostHumanQueryPayload):
    """
    Process a natural language query, convert it to SQL, execute the query,
    and return a natural language response.
    """
    logger.info(f"Received query: {payload.human_query}")

    # Convert natural language to SQL
    sql_response = await human_query_to_sql(payload.human_query)
    
    if not sql_response:
        return {"error": "Failed to generate SQL query"}
    
    try:
        result_dict = json.loads(sql_response)
        sql_query = result_dict.get("sql_query", "")
        
        if not sql_query:
            return {"error": "No SQL query generated"}
        
        logger.info(f"Generated SQL: {sql_query}")
        
        # Execute the SQL query
        result = await query(sql_query)
        
        # Generate natural language response
        answer = await build_answer(result, payload.human_query)
        
        if not answer:
            return {"error": "Failed to generate response"}
        
        logger.info(f"Generated answer for query")
        return {"answer": answer, "sql": sql_query, "data": result}
        
    except json.JSONDecodeError:
        return {"error": "Invalid JSON response from SQL generation"}
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return {"error": f"Error processing query: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    
    # Set up logging
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    
    # Start the server
    uvicorn.run(app, host="0.0.0.0", port=8000)