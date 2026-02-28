import json
import logging
import os

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from sqlalchemy.orm import Session

from src.agent.tools import build_tools

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are an expert AI Data Analyst for the Kazakhstan Public Procurement system (goszakup.gov.kz).
You MUST use the provided tools to extract statistical facts. NEVER calculate math yourself.

STRICT RESPONSE FORMAT (Respond in the language of the user, KZ or RU):
1. Краткий вывод: (1-3 sentences stating if there is an anomaly, overpricing, or normal behavior).
2. Использованные данные: (State the KTRU, BIN, or filters used).
3. Сравнение: (Provide the specific median, weighted average, and deviation percentages).
4. Метрика оценки: (State if IQR or Weighted Average was used).
5. Ограничения и уверенность: (Mention the sample size and data quality).
6. Примеры: (Provide a bulleted list of the Top-K direct links returned by the tool).
"""

async def process_user_query(user_prompt: str, db: Session) -> str:
    logger.info(f"Received User Prompt: {user_prompt}")

    tools = build_tools(db)
    logger.info(f"Initialized {len(tools)} tools for the agent")

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0.1,
        api_key=os.getenv("OPENAI_API_KEY"),
    )
    llm_with_tools = llm.bind_tools(tools)

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_prompt),
    ]

    logger.info("Calling LLM to decide on tool usage")
    first_response = await llm_with_tools.ainvoke(messages)

    tool_calls = getattr(first_response, "tool_calls", None) or []
    if not tool_calls:
        logger.info("LLM decided no tools were needed.")
        return first_response.content or ""

    logger.info("LLM requested %d tool call(s)", len(tool_calls))

    tool_map = {tool.name: tool for tool in tools}

    tool_messages: list[ToolMessage] = []
    for call in tool_calls:
        name = getattr(call, "name", None) or call.get("name")
        args = getattr(call, "args", None) or call.get("args", {})
        call_id = getattr(call, "id", None) or call.get("id")

        logger.info(f"Executing tool '{name}' with args={args}")
        tool = tool_map.get(name)
        if tool is None:
            logger.warning(f"Requested unknown tool '{name}'")
            result = {"error": f"Unknown tool '{name}'."}
        else:
            try:
                result = tool.invoke(args)
            except Exception as e:
                logger.exception(f"Error while executing tool '{name}'")
                result = {"error": str(e)}

        content = result if isinstance(result, str) else json.dumps(result)
        tool_messages.append(
            ToolMessage(
                content=content,
                tool_call_id=str(call_id) if call_id is not None else "",
            )
        )

    final_messages = messages + [first_response] + tool_messages
    logger.info("Asking LLM to format final response")
    final_response = await llm.ainvoke(final_messages)
    logger.info("Final response generated.")

    return final_response.content or ""