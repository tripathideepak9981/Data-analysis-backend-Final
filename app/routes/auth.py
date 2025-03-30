import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from app.config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, DATABASE_URI
from app.database import SessionLocal
from app.models import User
 
router = APIRouter()
logger = logging.getLogger("auth")
logger.setLevel(logging.INFO)
 
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")
 
# Pydantic models.
class UserCreate(BaseModel):
    email: EmailStr
    username: str
    password: str
 
class Token(BaseModel):
    access_token: str
    token_type: str
 
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
 
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)
 
def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)
 
def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
 
def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.username == username).first()
 
def get_user_by_email(db: Session, email: str):
    return db.query(User).filter(User.email == email).first()
 
def create_dynamic_database_for_user(user: User) -> str:
    """
    Create a dynamic database based on the username.
    For example, if username is "deepak", the database name will be "deepak_db".
    """
    db_name = f"{user.username.strip().lower()}_db"
    engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name};"))
    logger.info(f"Dynamic database '{db_name}' created for user '{user.username}'.")
    return db_name
 
@router.post("/signup", response_model=Token)
def signup(user: UserCreate, db: Session = Depends(get_db)):
    if get_user_by_email(db, user.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    if get_user_by_username(db, user.username):
        raise HTTPException(status_code=400, detail="Username already registered")
   
    hashed_password = get_password_hash(user.password)
    # Note: dynamic_db is set to empty string here.
    new_user = User(email=user.email, username=user.username, hashed_password=hashed_password, dynamic_db="")
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
   
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": new_user.username, "user_id": new_user.id},
        expires_delta=access_token_expires,
    )
    logger.info(f"User '{new_user.username}' signed up successfully. No dynamic database created yet.")
    return {"access_token": access_token, "token_type": "bearer"}
 
@router.post("/login", response_model=Token)

def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):

    # Use the 'username' field from the form as the email address.

    user = get_user_by_email(db, form_data.username)

    if not user or not verify_password(form_data.password, user.hashed_password):

        raise HTTPException(status_code=400, detail="Incorrect email or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(

        data={"sub": user.username, "user_id": user.id},

        expires_delta=access_token_expires,

    )

    logger.info(f"User '{user.username}' logged in successfully using email.")

    return {"access_token": access_token, "token_type": "bearer"}
 
def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        if username is None or user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user_by_username(db, username)
    if user is None:
        raise credentials_exception
    return user
 
@router.post("/logout")
def logout(response: Response, current_user: User = Depends(get_current_user)):
    """
    In a stateless JWT approach, logout is handled on the client side by removing the token.
    If using cookies, clear the cookie here.
    """
    response.delete_cookie("access_token")
    return {"detail": "Successfully logged out"}
 
 