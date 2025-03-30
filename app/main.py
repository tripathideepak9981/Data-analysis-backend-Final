# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import auth,upload, db, query, join, modify,chart
 
app = FastAPI(title="AI Data Analysis Chatbot API")
 
# Allow CORS for the React frontend (adjust allowed origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
 
# Include routers under a common prefix (e.g., /api)
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(upload.router, prefix="/api")
app.include_router(db.router, prefix="/api")
app.include_router(query.router, prefix="/api")
app.include_router(join.router, prefix="/api")
app.include_router(modify.router, prefix="/api")
app.include_router(chart.router, prefix="/chart", tags=["Chart"])
 
 
 
@app.get("/")
def root():
    return {"message": "Welcome to the AI Data Analysis Chatbot API"}
 
 