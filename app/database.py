# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import DATABASE_URI
 
# Create the engine for the main (central) database.
engine = create_engine(DATABASE_URI)
 
# Create a sessionmaker bound to this engine.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
 
# Dependency function that yields a session.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
 
 