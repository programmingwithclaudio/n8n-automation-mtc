import asyncio
import json
import requests
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "localpassword"
DB_PORT = 5432

# API endpoint
API_URL = "http://localhost:8000/human_query"

# Function to create test tables and insert sample data
def setup_test_database():
    """Create test tables and insert sample data into the PostgreSQL database."""
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Drop tables if they exist
        cur.execute("DROP TABLE IF EXISTS orders;")
        cur.execute("DROP TABLE IF EXISTS customers;")
        cur.execute("DROP TABLE IF EXISTS products;")
        
        # Create customers table
        cur.execute("""
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                registration_date DATE NOT NULL,
                city VARCHAR(50),
                country VARCHAR(50)
            );
        """)
        
        # Create products table
        cur.execute("""
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                price DECIMAL(10, 2) NOT NULL,
                category VARCHAR(50),
                stock_quantity INTEGER DEFAULT 0
            );
        """)
        
        # Create orders table
        cur.execute("""
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(customer_id),
                order_date DATE NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending'
            );
        """)
        
        # Insert sample data into customers
        customers_data = [
            (1, 'John Doe', 'john@example.com', '2023-01-15', 'New York', 'USA'),
            (2, 'Jane Smith', 'jane@example.com', '2023-02-20', 'London', 'UK'),
            (3, 'Carlos Rodriguez', 'carlos@example.com', '2023-03-10', 'Madrid', 'Spain'),
            (4, 'Maria Garcia', 'maria@example.com', '2023-04-05', 'Mexico City', 'Mexico'),
            (5, 'Hiroshi Tanaka', 'hiroshi@example.com', '2023-05-12', 'Tokyo', 'Japan')
        ]
        
        cur.executemany("""
            INSERT INTO customers (customer_id, name, email, registration_date, city, country)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, customers_data)
        
        # Insert sample data into products
        products_data = [
            (1, 'Laptop', 'High-performance laptop', 1200.00, 'Electronics', 50),
            (2, 'Smartphone', 'Latest model smartphone', 800.00, 'Electronics', 100),
            (3, 'Coffee Maker', 'Automatic coffee machine', 150.00, 'Home Appliances', 30),
            (4, 'Desk Chair', 'Ergonomic office chair', 250.00, 'Furniture', 20),
            (5, 'Headphones', 'Noise-cancelling headphones', 180.00, 'Electronics', 75)
        ]
        
        cur.executemany("""
            INSERT INTO products (product_id, name, description, price, category, stock_quantity)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, products_data)
        
        # Insert sample data into orders
        orders_data = [
            (1, 1, '2023-06-10', 1200.00, 'completed'),
            (2, 2, '2023-06-15', 980.00, 'completed'),
            (3, 3, '2023-06-20', 150.00, 'processing'),
            (4, 1, '2023-07-05', 180.00, 'completed'),
            (5, 4, '2023-07-10', 250.00, 'pending'),
            (6, 5, '2023-07-15', 800.00, 'processing'),
            (7, 2, '2023-07-20', 330.00, 'completed'),
            (8, 3, '2023-08-01', 1200.00, 'pending')
        ]
        
        cur.executemany("""
            INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
            VALUES (%s, %s, %s, %s, %s);
        """, orders_data)
        
        # Commit the changes
        conn.commit()
        
        print("Test database setup completed successfully.")
        
    except Exception as e:
        print(f"Error setting up test database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Function to test the API with sample queries
def test_api_with_queries():
    """Test the AI agent API with a set of sample natural language queries."""
    test_queries = [
        "What are all the customers in our database?",
        "Show me all products in the Electronics category",
        "What is the total revenue from completed orders?",
        "Who is our customer with the highest total order amount?",
        "How many orders are currently in 'pending' status?"
    ]
    
    for query in test_queries:
        try:
            # Make the API request
            response = requests.post(
                API_URL,
                json={"human_query": query}
            )
            
            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                print(f"\nQuery: {query}")
                print(f"Answer: {result.get('answer', 'No answer provided')}")
            else:
                print(f"\nError for query '{query}': {response.status_code}")
                print(response.text)
                
        except Exception as e:
            print(f"\nException occurred for query '{query}': {e}")

# Function to generate a curl command for testing
def generate_curl_command():
    """Generate a curl command that can be used to test the API."""
    curl_command = """
curl -X POST \\
  http://localhost:8000/human_query \\
  -H 'Content-Type: application/json' \\
  -d '{"human_query": "List all customers from the USA"}'
"""
    print("\nCURL command for testing:")
    print(curl_command)

# Main function to run all tests
def main():
    # Setup the test database
    print("Setting up test database...")
    setup_test_database()
    
    # Generate curl command
    generate_curl_command()
    
    # Test the API
    print("\nTesting API with sample queries...")
    test_api_with_queries()

if __name__ == "__main__":
    main()