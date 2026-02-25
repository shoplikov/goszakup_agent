from pydantic import BaseModel
from sqlalchemy.orm import Session
from fastapi import FastAPI, Depends, HTTPException

from src.db.session import SessionLocal
from src.agent.llm import process_user_query

app = FastAPI(
    title="Goszakup AI Agent API",
    description="AI-powered analysis of Kazakhstan public procurement data.",
    version="1.0.0"
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    answer: str

@app.post("/ask", response_model=QueryResponse)
async def ask_agent(request: QueryRequest, db: Session = Depends(get_db)):
    """
    Example input: 'Оцени адекватность цены по КТРУ 259923.300.000000'
    """
    try:
        answer = await process_user_query(request.question, db)
        return QueryResponse(answer=answer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "Goszakup AI Agent is running."}