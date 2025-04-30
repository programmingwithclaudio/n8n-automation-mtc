
#!/bin/bash

# Test the AI agent API with curl

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Testing AI Agent API...${NC}"

# Test 1: List all customers
echo -e "\n${YELLOW}Test 1: List all customers${NC}"
curl -X POST \
  http://localhost:8000/human_query \
  -H 'Content-Type: application/json' \
  -d '{"human_query": "List all customers"}' \
  -s | jq

# Test 2: Products in Electronics category
echo -e "\n${YELLOW}Test 2: Products in Electronics category${NC}"
curl -X POST \
  http://localhost:8000/human_query \
  -H 'Content-Type: application/json' \
  -d '{"human_query": "What products are in the Electronics category?"}' \
  -s | jq

# Test 3: Total revenue from completed orders
echo -e "\n${YELLOW}Test 3: Total revenue from completed orders${NC}"
curl -X POST \
  http://localhost:8000/human_query \
  -H 'Content-Type: application/json' \
  -d '{"human_query": "What is the total revenue from completed orders?"}' \
  -s | jq

# Test 4: Customer with highest total order amount
echo -e "\n${YELLOW}Test 4: Customer with highest total order amount${NC}"
curl -X POST \
  http://localhost:8000/human_query \
  -H 'Content-Type: application/json' \
  -d '{"human_query": "Which customer has spent the most money?"}' \
  -s | jq

# Test 5: Orders with pending status
echo -e "\n${YELLOW}Test 5: Orders with pending status${NC}"
curl -X POST \
  http://localhost:8000/human_query \
  -H 'Content-Type: application/json' \
  -d '{"human_query": "How many orders are pending?"}' \
  -s | jq

echo -e "\n${GREEN}API testing completed!${NC}"