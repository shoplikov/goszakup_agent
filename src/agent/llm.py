import os
from openai import AsyncOpenAI
from sqlalchemy.orm import Session
from src.agent.tools import FAIR_PRICE_TOOL_SCHEMA, execute_tool

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
You are an AI analytical agent for Kazakhstan public procurement (goszakup.gov.kz).
You analyze contracts, find anomalies, and evaluate price fairness.

CRITICAL RULES:
1. Do not calculate math yourself. ALWAYS use the provided tools to get statistical data.
2. Respond in the same language as the user (KZ or RU).
3. Base your answers strictly on the tool outputs. Do not invent data.

REQUIRED RESPONSE FORMAT:
Your final response MUST strictly follow this exact structure:
1. Вердикт: (1-3 sentences summarizing if the price is fair or anomalous).
2. Параметры поиска: (What KTRU/Filters were used).
3. Аналитика: (State the median, fair range bounds, and the compared price).
4. Уверенность: (State the confidence level based on sample size).
5. Детализация: (List identifiers or extra context if available).
"""

async def process_user_query(user_prompt: str, db: Session) -> str:
    """
    Asynchronously processes a natural language query, orchestrates tool calls, 
    and returns a formatted analytical response.
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt}
    ]

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=[FAIR_PRICE_TOOL_SCHEMA],
        tool_choice="auto",
        temperature=0.1
    )
    
    response_message = response.choices[0].message

    # Step 2: Check if the LLM decided to use a tool
    if response_message.tool_calls:
        messages.append(response_message)
        
        for tool_call in response_message.tool_calls:
            tool_result = execute_tool(
                tool_name=tool_call.function.name,
                arguments=tool_call.function.arguments,
                db=db
            )
            
            # Feed the factual result back to the LLM
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": tool_call.function.name,
                "content": tool_result
            })
            
        # Step 3: Get the final natural language summary from the LLM
        final_response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.1
        )
        return final_response.choices[0].message.content

    return response_message.content