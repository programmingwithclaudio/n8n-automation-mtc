import json
import logging
import os
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import httpx
from typing import Dict, Any, List, Optional
import re

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "testdbauren")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "localpassword")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# FastAPI app
app = FastAPI(title="SQL Agent API")

class QueryRequest(BaseModel):
    human_query: str
    date: Optional[str] = None  # Optional date parameter

class SQLQueryResult(BaseModel):
    original_query: str
    sql_query: str
    result: List[Dict[str, Any]]
    error: Optional[str] = None

# Database connection function
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# Test database connection
def test_db_connection():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

# Function to get schema information
def get_database_schema():
    try:
        conn = get_db_connection()
        schema_info = {}
        
        # Get tables
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                schema_info[table_name] = []
                
                # Get columns for each table
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                
                for column in columns:
                    schema_info[table_name].append({
                        "column_name": column[0],
                        "data_type": column[1]
                    })
        
        conn.close()
        return schema_info
    except Exception as e:
        logger.error(f"Error fetching database schema: {e}")
        return {"error": str(e)}

# LLM function to convert natural language to SQL
async def convert_to_sql(query: str, date: Optional[str] = None):
    try:
        # Get schema information
        schema_info = get_database_schema()
        
        # Create system prompt with schema information
        system_prompt = f"""
        You are a PostgreSQL expert that converts natural language queries to SQL.
        
        Your task is to:
        1. Understand the user's query in natural language
        2. Generate the appropriate SQL query for PostgreSQL
        3. Return ONLY the SQL query without any explanations or markdown
        
        Here's the database schema information:
        {json.dumps(schema_info, indent=2)}
        
        Example queries to understand:
        - "Reporte por supervisor para un día dado" would generate:
          SELECT 
            vu.zonal, 
            vu.supervisor, 
            SUM(vu.hc) AS total_hc, 
            SUM(vu.login) AS total_login, 
            SUM(vu.asisth) AS total_asisth, 
            SUM(vu.hc_c_vta) AS total_hc_c_vta, 
            SUM(vu.ventas) AS total_ventas, 
            MAX(vu.couta) AS couta,
            ROUND(SUM(vu.ventas) / NULLIF(MAX(vu.couta), 0), 2) AS cobertura
          FROM public.vista_actividad_usuarios vu
          WHERE 
            vu.fecha = '2025-05-06'
            AND vu.supervisor IS NOT NULL
          GROUP BY vu.zonal, vu.supervisor
          ORDER BY vu.zonal, vu.supervisor;
        
        - "Reporte por zonal sumando para cada supervisor su cuota máxima" would generate:
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
            FROM public.vista_actividad_usuarios vu
            WHERE vu.fecha = '2025-05-06'
            GROUP BY vu.zonal, vu.supervisor
          ) sub
          GROUP BY sub.zonal
          ORDER BY sub.zonal;
        
        Return ONLY the SQL query without any explanations, comments or markdown formatting.
        """
        
        # Prepare request for LLM API
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
        }
        
        # Add date information to the user query if provided
        user_query = query
        if date:
            user_query += f" para la fecha {date}"
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            "temperature": 0.1,  # Low temperature for more deterministic output
            "max_tokens": 1000
        }
        
        # Call the LLM API
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                DEEPSEEK_API_URL,
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                logger.error(f"LLM API error: {response.text}")
                return {"error": f"LLM API error: {response.status_code}", "sql": ""}
            
            response_data = response.json()
            sql_query = response_data["choices"][0]["message"]["content"].strip()
            
            # Clean the SQL query (remove markdown code blocks if present)
            sql_query = re.sub(r'^```sql\s*|\s*```$', '', sql_query, flags=re.MULTILINE)
            
            return {"error": None, "sql": sql_query}
    
    except Exception as e:
        logger.error(f"Error in convert_to_sql: {e}")
        return {"error": str(e), "sql": ""}

# Execute SQL query
def execute_sql_query(sql_query: str):
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            # Convert results to a list of dictionaries
            results_list = [dict(row) for row in results]
        conn.close()
        return {"error": None, "results": results_list}
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        return {"error": str(e), "results": []}

# API endpoints
@app.get("/health", tags=["Health"])
async def health_check():
    """Check if the API and database connection are working."""
    db_status = "Connected" if test_db_connection() else "Failed"
    return {
        "status": "ok",
        "database": db_status,
        "version": "1.0.0"
    }

@app.get("/schema", tags=["Database"])
async def get_schema():
    """Get database schema information."""
    schema = get_database_schema()
    return schema

@app.post("/human_query", response_model=SQLQueryResult, tags=["Query"])
async def process_human_query(request: QueryRequest):
    """
    Process a natural language query, convert it to SQL, execute it, and return results.
    """
    human_query = request.human_query
    date = request.date
    
    # Step 1: Convert natural language to SQL
    sql_response = await convert_to_sql(human_query, date)
    
    if sql_response["error"]:
        return SQLQueryResult(
            original_query=human_query,
            sql_query="",
            result=[],
            error=sql_response["error"]
        )
    
    sql_query = sql_response["sql"]
    
    # Step 2: Execute SQL query
    execution_result = execute_sql_query(sql_query)
    
    if execution_result["error"]:
        return SQLQueryResult(
            original_query=human_query,
            sql_query=sql_query,
            result=[],
            error=execution_result["error"]
        )
    
    # Step 3: Return results
    return SQLQueryResult(
        original_query=human_query,
        sql_query=sql_query,
        result=execution_result["results"],
        error=None
    )

@app.post("/execute_sql", tags=["Query"])
async def execute_direct_sql(request: Request):
    """Execute a SQL query directly."""
    try:
        body = await request.json()
        sql_query = body.get("sql_query", "")
        
        if not sql_query:
            raise HTTPException(status_code=400, detail="SQL query is required")
        
        execution_result = execute_sql_query(sql_query)
        
        if execution_result["error"]:
            raise HTTPException(status_code=400, detail=execution_result["error"])
        
        return {
            "sql_query": sql_query,
            "result": execution_result["results"]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in execute_direct_sql: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/whatsapp_query", tags=["WhatsApp"])
async def process_whatsapp_query(request: Request):
    """
    Process a query coming from WhatsApp.
    This could be integrated with a WhatsApp API service.
    """
    try:
        body = await request.json()
        message = body.get("message", "")
        
        if not message:
            raise HTTPException(status_code=400, detail="Message is required")
        
        # Extract date from message if present
        date_match = re.search(r'\b\d{4}-\d{2}-\d{2}\b', message)
        date = date_match.group(0) if date_match else None
        
        # Process the query
        sql_response = await convert_to_sql(message, date)
        
        if sql_response["error"]:
            return {
                "success": False,
                "message": f"Error converting to SQL: {sql_response['error']}",
                "original_query": message
            }
        
        sql_query = sql_response["sql"]
        execution_result = execute_sql_query(sql_query)
        
        if execution_result["error"]:
            return {
                "success": False,
                "message": f"Error executing SQL: {execution_result['error']}",
                "original_query": message,
                "sql_query": sql_query
            }
        
        return {
            "success": True,
            "original_query": message,
            "sql_query": sql_query,
            "result": execution_result["results"]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_whatsapp_query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)