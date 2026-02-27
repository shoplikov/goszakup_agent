import os
import logging
from openai import AsyncOpenAI
from sqlalchemy.orm import Session
from src.agent.tools import TOOLS_SCHEMA, execute_tool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt}
    ]

    logger.info("Sending query to LLM to determine intent")
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=TOOLS_SCHEMA,
        tool_choice="auto",
        temperature=0.1
    )
    
    response_message = response.choices[0].message

    if response_message.tool_calls:
        messages.append(response_message)
        
        for tool_call in response_message.tool_calls:
            tool_result = execute_tool(
                tool_name=tool_call.function.name,
                arguments=tool_call.function.arguments,
                db=db
            )
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": tool_call.function.name,
                "content": tool_result
            })
            
        logger.info("Tool data injected. Asking LLM to format final response")
        final_response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.1
        )
        logger.info("Final response generated.")
        return final_response.choices[0].message.content

    logger.info("LLM decided no tools were needed.")
    return response_message.content