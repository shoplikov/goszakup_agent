from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.agent.llm import process_user_query

app = FastAPI(title="Goszakup AI Agent")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    answer: str

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/ask", response_model=QueryResponse)
async def ask_agent(request: QueryRequest, db: Session = Depends(get_db)):
    try:
        answer = await process_user_query(request.question, db)
        return QueryResponse(answer=answer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))