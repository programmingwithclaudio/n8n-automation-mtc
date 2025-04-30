import json
import aiohttp
import logging
import os
from fastapi import FastAPI
from openai import BaseModel
from dotenv import load_dotenv
import os
from typing import Any
import openai

load_dotenv()


logger = logging.getLogger(__name__)

BACKEND_SERVER = os.getenv("SERVER_URL")

app = FastAPI(servers=[{"url": BACKEND_SERVER}])

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL")

def get_schema():
    # tu función para obtener el esquema de la base de datos
    pass

def query(sql_query: str):
    # tu función para hacer la consulta a la base de datos
    pass

async def human_query_to_sql(human_query: str) -> dict:
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

    async with aiohttp.ClientSession() as session:
        async with session.post(
            DEEPSEEK_API_URL,
            headers=headers,
            json=payload
        ) as response:
            response.raise_for_status()
            response_data = await response.json()
            return json.loads(response_data['choices'][0]['message']['content'])


async def build_answer(result: list[dict[str, Any]], human_query: str) -> str:
    system_message = f"""
    Given a users question and the SQL rows response from the database from which the user wants to get the answer,
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
            {"role": "system", "content": system_message}
        ],
        "temperature": 0.7
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
            response.raise_for_status()
            response_data = await response.json()
            return response_data['choices'][0]['message']['content']


class PostHumanQueryPayload(BaseModel):
    human_query: str


class PostHumanQueryResponse(BaseModel):
    result: list


@app.post(
    "/human_query",
    name="Human Query",
    operation_id="post_human_query",
    description="Gets a natural language query, internally transforms it to a SQL query, queries the database, and returns the result.",
)
async def human_query(payload: PostHumanQueryPayload):

    # Transforma la pregunta a sentencia SQL
    sql_query = await human_query_to_sql(payload.human_query)

    if not sql_query:
        return {"error": "Falló la generación de la consulta SQL"}
    result_dict = json.loads(sql_query)

    # Hace la consulta a la base de datos
    result = await query(result_dict["sql_query"])

    # Transforma la respuesta SQL a un formato más humano
    answer = await build_answer(result, payload.human_query)
    if not answer:
        return {"error": "Falló la generación de la respuesta"}

    return {"answer": answer}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)