from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn

from backend.app.core.workflow_engine import WorkflowEngine
from backend.app.core.intent_analyzer import IntentAnalyzer
from backend.app.core.task_planner import TaskPlanner
from backend.app.core.vector_db import VectorDB
from backend.app.core.reasoning_model import ReasoningModel
from backend.app.core.preference_model import PreferenceModel

app = FastAPI(title="AI Workflow Platform", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize core components
workflow_engine = WorkflowEngine()
intent_analyzer = IntentAnalyzer()
task_planner = TaskPlanner()
vector_db = VectorDB()
reasoning_model = ReasoningModel()
preference_model = PreferenceModel()

class WorkflowRequest(BaseModel):
    query: str
    user_id: Optional[str] = "default"

class FeedbackRequest(BaseModel):
    workflow_id: str
    rating: int
    feedback: Optional[str] = None

@app.post("/api/workflow/execute")
async def execute_workflow(request: WorkflowRequest):
    """执行AI工作流"""
    try:
        result = await workflow_engine.execute(
            query=request.query,
            user_id=request.user_id,
            intent_analyzer=intent_analyzer,
            task_planner=task_planner,
            vector_db=vector_db,
            reasoning_model=reasoning_model
        )
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/feedback")
async def submit_feedback(request: FeedbackRequest):
    """提交用户反馈"""
    try:
        await preference_model.update_preferences(
            workflow_id=request.workflow_id,
            rating=request.rating,
            feedback=request.feedback
        )
        return {"success": True, "message": "反馈已提交"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "message": "AI Workflow Platform is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)