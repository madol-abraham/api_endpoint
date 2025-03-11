from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from pydantic import BaseModel, Field
from typing import List, Optional

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb+srv://Madol:Madol%40567@cluster0.xsw82.mongodb.net/")
db = client["netflix_db"]
movies_collection = db["Movies"]

# Pydantic model for Movie
class Movie(BaseModel):
    movie_id: int
    title: str
    movie_type: str
    netflix_exclusive: str
    release_date: str

class MovieUpdate(BaseModel):
    title: Optional[str] = None
    movie_type: Optional[str] = None
    netflix_exclusive: Optional[str] = None
    release_date: Optional[str] = None
# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to Netflix!"}
# Create (POST) endpoint
@app.post("/movies/", response_model=Movie)
async def create_movie(movie: Movie):
    try:
        movie_dict = movie.dict()
        movies_collection.insert_one(movie_dict)
        return movie
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="Movie ID already exists")

# Read (GET) endpoint
@app.get("/movies/", response_model=List[Movie])
async def read_movies():
    movies = list(movies_collection.find())
    return movies

@app.get("/movies/{movie_id}", response_model=Movie)
async def read_movie(movie_id: int):
    movie = movies_collection.find_one({"movie_id": movie_id})
    if movie:
        return movie
    raise HTTPException(status_code=404, detail="Movie not found")

# Update (PUT) endpoint
@app.put("/movies/{movie_id}", response_model=Movie)
async def update_movie(movie_id: int, movie_update: MovieUpdate):
    update_data = {k: v for k, v in movie_update.dict().items() if v is not None}
    if update_data:
        result = movies_collection.update_one({"movie_id": movie_id}, {"$set": update_data})
        if result.matched_count:
            updated_movie = movies_collection.find_one({"movie_id": movie_id})
            return updated_movie
    raise HTTPException(status_code=404, detail="Movie not found")

# Delete (DELETE) endpoint
@app.delete("/movies/{movie_id}", response_model=dict)
async def delete_movie(movie_id: int):
    result = movies_collection.delete_one({"movie_id": movie_id})
    if result.deleted_count:
        return {"status": "success", "message": "Movie deleted"}
    raise HTTPException(status_code=404, detail="Movie not found")

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)