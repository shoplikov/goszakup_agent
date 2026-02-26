import json
from sqlalchemy.orm import Session
from src.analytics.fair_price import get_fair_price_bounds

FAIR_PRICE_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "check_fair_price",
        "description": "Calculates the statistical fair price range for a specific item using its KTRU (ЕНСТРУ) code. Use this when the user asks about price fairness, anomalies, or adequacy.",
        "parameters": {
            "type": "object",
            "properties": {
                "enstru_code": {
                    "type": "string",
                    "description": "The exact KTRU code of the item, e.g., '259923.300.000000'"
                },
                "kato_code": {
                    "type": "string",
                    "description": "The regional KATO code, if specified by the user."
                }
            },
            "required": ["enstru_code"]
        }
    }
}

def execute_tool(tool_name: str, arguments: str, db: Session) -> str:
    args = json.loads(arguments)
    
    if tool_name == "check_fair_price":
        enstru_code = args.get("enstru_code")
        kato_code = args.get("kato_code")
        
        result = get_fair_price_bounds(db, enstru_code=enstru_code, kato_code=kato_code)
        if not result:
            return json.dumps({"error": f"No historical pricing data found for KTRU {enstru_code}."})
            
        return result.model_dump_json()
        
    return json.dumps({"error": "Unknown tool called."})