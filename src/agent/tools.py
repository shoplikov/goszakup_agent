import json
import logging
from typing import List

from langchain_core.tools import tool
from sqlalchemy.orm import Session

from src.analytics.engine import (
    analyze_price_dynamics,
    check_price_deviation,
    detect_volume_anomaly,
    get_fair_price_bounds,
    get_top_contracts,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def build_tools(db: Session) -> List:
    @tool(
        name="check_price_deviation",
        description=(
            "Checks if a specific price for a KTRU code deviates more than 30% "
            "from the historical weighted average."
        ),
    )
    def check_price_deviation_tool(enstru_code: str, target_price: float) -> dict:
        """Check price deviation for a KTRU code."""
        logger.info(
            f"Tool 'check_price_deviation' called with enstru_code={enstru_code}, target_price={target_price}"
        )
        try:
            res = check_price_deviation(db, enstru_code, target_price)
            if res is None:
                return {"error": "No data found for this KTRU."}
            return res.model_dump()
        except Exception as e:
            logger.exception("Error in 'check_price_deviation' tool")
            return {"error": str(e)}

    @tool(
        name="detect_volume_anomaly",
        description=(
            "Detects atypical inflation in the volume/quantity of goods purchased by a "
            "specific customer compared to previous years."
        ),
    )
    def detect_volume_anomaly_tool(customer_bin: str, enstru_code: str) -> dict:
        """Detect volume anomalies for a customer BIN and KTRU code."""
        logger.info(
            f"Tool 'detect_volume_anomaly' called with customer_bin={customer_bin}, enstru_code={enstru_code}"
        )
        try:
            res = detect_volume_anomaly(db, customer_bin, enstru_code)
            if res is None:
                return {"error": "No historical volume data found."}
            return res.model_dump()
        except Exception as e:
            logger.exception("Error in 'detect_volume_anomaly' tool")
            return {"error": str(e)}

    @tool(
        name="get_fair_price",
        description=(
            "Calculates the statistical fair price (IQR) and median for an item, "
            "optionally filtered by region (KATO) or year."
        ),
    )
    def get_fair_price_tool(
        enstru_code: str,
        kato_code: str | None = None,
        year_filter: int | None = None,
    ) -> dict:
        """Get statistical fair price bounds for a KTRU code."""
        logger.info(
            f"Tool 'get_fair_price' called with enstru_code={enstru_code}, kato_code={kato_code}, year_filter={year_filter}"
        )
        try:
            res = get_fair_price_bounds(db, enstru_code, kato_code, year_filter)
            if res is None:
                return {"error": "Insufficient data to calculate fair price."}
            return res.model_dump()
        except Exception as e:
            logger.exception("Error in 'get_fair_price' tool")
            return {"error": str(e)}

    @tool(
        name="analyze_price_dynamics",
        description=(
            "Analyzes historical monthly prices for a KTRU code to reveal inflation "
            "and seasonal patterns."
        ),
    )
    def analyze_price_dynamics_tool(enstru_code: str) -> dict:
        """Analyze historical monthly price dynamics for a KTRU code."""
        logger.info(
            f"Tool 'analyze_price_dynamics' called with enstru_code={enstru_code}"
        )
        try:
            res = analyze_price_dynamics(db, enstru_code)
            return res
        except Exception as e:
            logger.exception("Error in 'analyze_price_dynamics' tool")
            return {"error": str(e)}

    @tool(
        name="get_top_contracts",
        description=(
            "Returns the Top-K most expensive contracts for a specific customer BIN."
        ),
    )
    def get_top_contracts_tool(customer_bin: str, limit: int = 5) -> list:
        """Get the Top-K most expensive contracts for a customer BIN."""
        logger.info(
            f"Tool 'get_top_contracts' called with customer_bin={customer_bin}, limit={limit}"
        )
        try:
            res = get_top_contracts(db, customer_bin, limit)
            return res
        except Exception as e:
            logger.exception("Error in 'get_top_contracts' tool")
            return [{"error": str(e)}]

    return [
        check_price_deviation_tool,
        detect_volume_anomaly_tool,
        get_fair_price_tool,
        analyze_price_dynamics_tool,
        get_top_contracts_tool,
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