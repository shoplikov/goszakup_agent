import json
from sqlalchemy.orm import Session
from src.analytics.engine import (
    check_price_deviation,
    detect_volume_anomaly,
    get_fair_price_bounds,
    analyze_price_dynamics,
    get_top_contracts,
)
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "check_price_deviation",
            "description": "Checks if a specific price for a KTRU code deviates more than 30% from the historical weighted average.",
            "parameters": {
                "type": "object",
                "properties": {
                    "enstru_code": {"type": "string", "description": "The KTRU code (e.g., '351210.900.000000')"},
                    "target_price": {"type": "number", "description": "The price to check"}
                },
                "required": ["enstru_code", "target_price"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "detect_volume_anomaly",
            "description": "Detects atypical inflation in the volume/quantity of goods purchased by a specific customer compared to previous years.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_bin": {"type": "string", "description": "The BIN of the customer organization"},
                    "enstru_code": {"type": "string", "description": "The KTRU code"}
                },
                "required": ["customer_bin", "enstru_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_fair_price",
            "description": "Calculates the statistical fair price (IQR) and median for an item, optionally filtered by region (KATO) or year.",
            "parameters": {
                "type": "object",
                "properties": {
                    "enstru_code": {"type": "string", "description": "The KTRU code"},
                    "kato_code": {"type": "string", "description": "Regional KATO code, if specified"},
                    "year_filter": {"type": "integer", "description": "Specific year, if specified"}
                },
                "required": ["enstru_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_price_dynamics",
            "description": "Analyzes historical monthly prices for a KTRU code to reveal inflation and seasonal patterns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "enstru_code": {"type": "string", "description": "The KTRU code (e.g., '351210.900.000000')"},
                },
                "required": ["enstru_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_contracts",
            "description": "Returns the Top-K most expensive contracts for a specific customer BIN.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_bin": {"type": "string", "description": "The BIN of the customer organization"},
                    "limit": {
                        "type": "integer",
                        "description": "Number of contracts to return (Top-K). Defaults to 5.",
                        "default": 5,
                    },
                },
                "required": ["customer_bin"]
            }
        }
    }
]

def execute_tool(tool_name: str, arguments: str, db: Session) -> str:
    args = json.loads(arguments)
    logger.info(f"Agent called tool: '{tool_name}' with args: {args}")
    
    try:
        if tool_name == "check_price_deviation":
            res = check_price_deviation(db, args["enstru_code"], args["target_price"])
            result_json = res.model_dump_json() if res else json.dumps({"error": "No data found for this KTRU."})
            
        elif tool_name == "detect_volume_anomaly":
            res = detect_volume_anomaly(db, args["customer_bin"], args["enstru_code"])
            result_json = res.model_dump_json() if res else json.dumps({"error": "No historical volume data found."})
            
        elif tool_name == "get_fair_price":
            res = get_fair_price_bounds(db, args["enstru_code"], args.get("kato_code"), args.get("year_filter"))
            result_json = res.model_dump_json() if res else json.dumps({"error": "Insufficient data to calculate fair price."})

        elif tool_name == "analyze_price_dynamics":
            res = analyze_price_dynamics(db, args["enstru_code"])
            result_json = json.dumps(res)

        elif tool_name == "get_top_contracts":
            res = get_top_contracts(db, args["customer_bin"], args.get("limit", 5))
            result_json = json.dumps(res)
            
        else:
            result_json = json.dumps({"error": "Unknown tool."})
            
    except Exception as e:
        result_json = json.dumps({"error": str(e)})
        
    logger.info(f"Data returned to Agent:\n{result_json}")
    return result_json