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
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "testdbauren")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "localpassword")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")  # Default schema

# Create FastAPI app
app = FastAPI(title="SQL Query Generator API")

def check_view_exists() -> bool:
    """
    Check if the vista_actividad_por_usuarios view exists in the database.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # Check if the view exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.views 
                WHERE table_schema = %s AND 
                      table_name = 'vista_actividad_por_usuarios'
            );
        """, (DB_SCHEMA,))
        
        exists = cur.fetchone()[0]
        
        # Log the result
        if exists:
            logger.info("View 'vista_actividad_por_usuarios' exists in schema '%s'", DB_SCHEMA)
        else:
            logger.warning("View 'vista_actividad_por_usuarios' does NOT exist in schema '%s'", DB_SCHEMA)
            
            # Try to find the view in other schemas
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.views 
                WHERE table_name = 'vista_actividad_por_usuarios'
            """)
            
            other_locations = cur.fetchall()
            if other_locations:
                logger.info("Found view in other schemas: %s", other_locations)
        
        cur.close()
        conn.close()
        return exists
        
    except Exception as e:
        logger.error(f"Error checking view existence: {e}")
        return False

def get_view_schema() -> str:
    """
    Get the schema of the vista_actividad_por_usuarios view.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        
        cur = conn.cursor()
        
        # First, find which schema the view is in
        cur.execute("""
            SELECT table_schema 
            FROM information_schema.views 
            WHERE table_name = 'vista_actividad_por_usuarios'
        """)
        
        schemas = cur.fetchall()
        if not schemas:
            logger.error("View 'vista_actividad_por_usuarios' not found in any schema")
            return "View not found"
            
        target_schema = schemas[0][0]  # Use the first schema found
        logger.info(f"Found view in schema: {target_schema}")
        
        # Get column information
        cur.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = 'vista_actividad_por_usuarios'
            ORDER BY ordinal_position
        """, (target_schema,))
        
        columns = cur.fetchall()
        
        schema_def = f"CREATE OR REPLACE VIEW {target_schema}.vista_actividad_por_usuarios AS (\n"
        schema_def += "  SELECT\n"
        
        column_defs = []
        for col in columns:
            column_name, data_type, is_nullable = col
            nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
            column_defs.append(f"    {column_name} {data_type} {nullable_str}")
            
        schema_def += ",\n".join(column_defs)
        schema_def += "\n  FROM source_table\n);"
        
        cur.close()
        conn.close()
        
        return schema_def
        
    except Exception as e:
        logger.error(f"Error getting view schema: {e}")
        return f"Error: {str(e)}"

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
            WHERE table_schema = %s
        """, (DB_SCHEMA,))
        
        tables = cur.fetchall()
        schema_info = []
        
        # Add information about the view
        view_schema = get_view_schema()
        if view_schema != "View not found" and not view_schema.startswith("Error:"):
            schema_info.append(view_schema)
        
        # For each table, get its columns and constraints
        for table in tables:
            table_name = table[0]
            
            # Get column information
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (DB_SCHEMA, table_name))
            
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
                WHERE constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s AND tc.table_name = %s
            """, (DB_SCHEMA, table_name))
            
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
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s AND tc.table_name = %s
            """, (DB_SCHEMA, table_name))
            
            foreign_keys = []
            for fk in cur.fetchall():
                column_name, foreign_table, foreign_column = fk
                foreign_keys.append(f"FOREIGN KEY ({column_name}) REFERENCES {foreign_table}({foreign_column})")
            
            # Compile table definition
            table_def = f"CREATE TABLE {DB_SCHEMA}.{table_name} (\n"
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
        logger.info(f"Executing SQL query: {sql_query}")
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
    # Check if the view exists first
    view_exists = check_view_exists()
    if not view_exists:
        logger.warning("The view 'vista_actividad_por_usuarios' does not exist in the database")
        
    # Get the actual view schema
    view_schema = get_view_schema()
    
    # Determine the correct schema name to use in queries
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT table_schema 
            FROM information_schema.views 
            WHERE table_name = 'vista_actividad_por_usuarios'
        """)
        schemas = cur.fetchall()
        actual_schema = DB_SCHEMA  # Default
        if schemas:
            actual_schema = schemas[0][0]
            logger.info(f"Using actual schema for view: {actual_schema}")
    except Exception as e:
        logger.error(f"Error determining schema: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    system_message = f"""
You are an expert PostgreSQL database developer. Your task is to translate natural language queries into valid PostgreSQL SQL queries.

Database information:
1. We have a view called '{actual_schema}.vista_actividad_por_usuarios' with these columns:
   - fecha (date): The date of the activity
   - dni (text): ID number of the user
   - nombre (text): Name of the user
   - zonal (text): Zone/region name
   - supervisor (text): Supervisor's name
   - estado (text): Status, e.g., "En campo"
   - rol (text): Role, e.g., "COMISIONISTA"
   - hc (integer): Head count
   - login (integer): Login count
   - asisth (integer): Assistance hours
   - hc_c_vta (integer): HC with sales
   - ventas (integer): Sales amount
   - couta (integer): Quota/target amount

Schema:
{view_schema}

Instructions:
1. Return ONLY a JSON object with a "sql_query" field containing the SQL query
2. Use PostgreSQL syntax
3. VERY IMPORTANT: Always use '{actual_schema}.vista_actividad_por_usuarios' (with the schema prefix) in your queries, not just 'vista_actividad_por_usuarios'
4. Make sure to handle division by zero with NULLIF() when calculating percentages or ratios
5. For date parameters, allow the date to be specified in the query or default to current date
6. Include appropriate aggregations (SUM, AVG, MAX) as needed
7. For supervisor or zonal reports, always group by those fields
8. Use the ROUND() function to limit decimal places in calculations
9. Always qualify column names with table aliases

Example 1 query: "Reporte por supervisor para un día dado"
Example 1 SQL:
```
SELECT 
  vu.zonal, 
  vu.supervisor, 
  SUM(vu.hc) AS total_hc, 
  SUM(vu.login) AS total_login, 
  SUM(vu.asisth) AS total_asisth, 
  SUM(vu.hc_c_vta) AS total_hc_c_vta, 
  SUM(vu.ventas) AS total_ventas, 
  MAX(vu.couta) AS couta,
  ROUND(SUM(vu.ventas)/NULLIF(MAX(vu.couta),0),2) AS cobertura
FROM {actual_schema}.vista_actividad_por_usuarios vu
WHERE vu.fecha = '2025-05-05'
GROUP BY vu.zonal, vu.supervisor
ORDER BY vu.zonal, vu.supervisor;
```

Example 2 query: "Reporte por zonal sumando para cada supervisor su cuota máxima"
Example 2 SQL:
```
SELECT 
  sub.zonal,
  SUM(sub.ventas) AS ventas,
  SUM(sub.max_couta) AS couta,
  ROUND(SUM(sub.ventas)/NULLIF(SUM(sub.max_couta),0),2) AS cobertura
FROM (
  SELECT 
    vu.zonal,
    vu.supervisor,
    MAX(vu.couta) AS max_couta,
    SUM(vu.ventas) AS ventas
  FROM {actual_schema}.vista_actividad_por_usuarios vu
  WHERE vu.fecha = '2025-05-05'
  GROUP BY vu.zonal, vu.supervisor
) sub
GROUP BY sub.zonal
ORDER BY sub.zonal;
```

Your response should be a valid JSON object with just the SQL query inside a 'sql_query' field.
"""
    
    try:
        # Ensure API key is available
        if not DEEPSEEK_API_KEY:
            logger.error("DEEPSEEK_API_KEY environment variable is not set")
            return json.dumps({
                "error": "API key not configured",
                "sql_query": ""
            })
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": f"Generate PostgreSQL query for: {human_query}"}
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }

        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DEEPSEEK_API_URL,
                headers=headers,
                json=payload
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"DeepSeek API error: {response.status} - {error_text}")
                    return json.dumps({
                        "error": f"API error: {response.status}",
                        "sql_query": ""
                    })
                
                response_data = await response.json()
                content = response_data['choices'][0]['message']['content']
                
                # Log the raw response for debugging
                logger.info(f"DeepSeek API raw response: {content}")
                
                return content
    except Exception as e:
        logger.error(f"Error calling DeepSeek API for SQL conversion: {e}")
        return json.dumps({
            "sql_query": "",
            "error": f"Failed to generate SQL: {str(e)}"
        })

async def build_answer(result: List[Dict[str, Any]], human_query: str, sql_query: str) -> str:
    """
    Build a natural language answer from SQL results using the DeepSeek API.
    """
    system_message = f"""
You are an expert data analyst. Your task is to provide a clear, informative response to the user's question based on the data results.

<user_question>
{human_query}
</user_question>

<sql_query>
{sql_query}
</sql_query>

<sql_response>
{json.dumps(result, indent=2)}
</sql_response>

Instructions:
1. Summarize the key insights from the data
2. If applicable, mention notable patterns, outliers, or trends
3. Be concise but thorough
4. Use simple language that a business user would understand
5. Format numbers appropriately (add thousands separators, limit decimals, etc.)
6. Do not mention the SQL query unless specifically asked about it
7. Focus on answering the original question
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
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"DeepSeek API error in build_answer: {response.status} - {error_text}")
                    return f"Error generating response: API returned status {response.status}"
                
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
        return {"error": "Failed to generate SQL query", "details": "No response from AI service"}
    
    try:
        # Parse the JSON response
        result_dict = json.loads(sql_response)
        
        # Check for errors in the response
        if "error" in result_dict and result_dict["error"]:
            logger.error(f"Error in SQL generation: {result_dict['error']}")
            return {"error": result_dict["error"]}
        
        # Get the SQL query
        sql_query = result_dict.get("sql_query", "")
        
        if not sql_query or sql_query.strip() == "":
            logger.error("Empty SQL query generated")
            return {"error": "No SQL query generated", "details": "The AI service returned an empty SQL query"}
        
        logger.info(f"Generated SQL: {sql_query}")
        
        # Execute the SQL query
        result = await query(sql_query)
        
        # Check if there was an error executing the query
        if result and len(result) == 1 and "error" in result[0]:
            logger.error(f"Error executing SQL: {result[0]['error']}")
            return {"error": "SQL execution failed", "details": result[0]["error"], "sql": sql_query}
        
        # Generate natural language response
        answer = await build_answer(result, payload.human_query, sql_query)
        
        if not answer:
            logger.error("Failed to generate natural language answer")
            return {"error": "Failed to generate response", "sql": sql_query, "data": result}
        
        logger.info(f"Generated answer for query")
        return {"answer": answer, "sql": sql_query, "data": result}
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response from SQL generation: {e} - Response: {sql_response}")
        return {"error": "Invalid JSON response from SQL generation", "details": str(e), "raw_response": sql_response}
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return {"error": f"Error processing query: {str(e)}"}

@app.get(
    "/health",
    name="Health Check",
    operation_id="health_check",
    description="Simple health check endpoint to verify the API is running.",
)
async def health_check():
    """
    Simple health check endpoint.
    """
    return {"status": "healthy", "timestamp": str(import_datetime_datetime.now())}

@app.get(
    "/test_view",
    name="Test View",
    operation_id="test_view",
    description="Test if the vista_actividad_por_usuarios view exists and is accessible.",
)
async def test_view():
    """
    Test if the view exists and can be queried.
    """
    try:
        # Determine the correct schema
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Find where the view is located
        cur.execute("""
            SELECT table_schema 
            FROM information_schema.views 
            WHERE table_name = 'vista_actividad_por_usuarios'
        """)
        
        schemas = cur.fetchall()
        if not schemas:
            return {"status": "error", "message": "View not found in any schema"}
        
        schema_name = schemas[0]['table_schema']
        
        # Test a simple query
        test_query = f"""
            SELECT * FROM {schema_name}.vista_actividad_por_usuarios LIMIT 3
        """
        
        cur.execute(test_query)
        result = cur.fetchall()
        
        # Clean up result for JSON serialization
        serializable_result = []
        for row in result:
            cleaned_row = {}
            for key, value in dict(row).items():
                if isinstance(value, (int, float, str, bool, type(None))):
                    cleaned_row[key] = value
                else:
                    cleaned_row[key] = str(value)
            serializable_result.append(cleaned_row)
        
        cur.close()
        conn.close()
        
        return {
            "status": "success", 
            "message": f"View found in schema: {schema_name}",
            "sample_data": serializable_result
        }
        
    except Exception as e:
        logger.error(f"Error testing view: {e}")
        return {"status": "error", "message": str(e)}

# Import datetime here to avoid circular import issues
import datetime as import_datetime_datetime

if __name__ == "__main__":
    import uvicorn
    
    # Set up logging
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    
    # Start the server
    uvicorn.run(app, host="0.0.0.0", port=8000)

