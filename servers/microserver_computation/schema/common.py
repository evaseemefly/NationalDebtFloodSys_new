from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import uvicorn

# 响应数据模型
class ResponseModel(BaseModel):
    code: int = Field(200, description="状态码")
    message: str = Field("success", description="响应信息")
    data: Optional[dict] = Field(None, description="响应数据")