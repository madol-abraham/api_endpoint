from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date
import pymysql
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# MySQL connection details
DB_HOST = "crossover.proxy.rlwy.net"
DB_USER = "root"
DB_PASSWORD = "kqHiuQhiEalRPNlbYTTpfqbymjzdCFEQ"
DB_NAME = "railway"
DB_PORT = 11902

# Establish MySQL connection
def get_db_connection():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

app = FastAPI()

# Pydantic model for Movie
class Movie(BaseModel):
    movie_id: int
    title: str
    movie_type: str
    netflix_exclusive: str
    release_date: date

class MovieUpdate(BaseModel):
    title: Optional[str] = None
    movie_type: Optional[str] = None
    netflix_exclusive: Optional[str] = None
    release_date: Optional[date] = None

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to Netflix!"}

# Favicon endpoint
@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    return Response(status_code=204)

# Create (POST) endpoint
@app.post("/movies/", response_model=Movie)
async def create_movie(movie: Movie):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO Movies (movie_id, title, movie_type, netflix_exclusive, release_date)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                movie.movie_id, movie.title, movie.movie_type,
                movie.netflix_exclusive, movie.release_date
            ))
            connection.commit()
        return movie
    except pymysql.err.IntegrityError:
        raise HTTPException(status_code=400, detail="Movie ID already exists")
    finally:
        connection.close()

# Read (GET) endpoint - All movies
@app.get("/movies/", response_model=List[Movie])
async def read_movies():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM Movies"
            cursor.execute(sql)
            movies = cursor.fetchall()
            return movies
    finally:
        connection.close()

# Read (GET) endpoint - Single movie
@app.get("/movies/{movie_id}", response_model=Movie)
async def read_movie(movie_id: int):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM Movies WHERE movie_id = %s"
            cursor.execute(sql, (movie_id,))
            movie = cursor.fetchone()
            if movie:
                return movie
            raise HTTPException(status_code=404, detail="Movie not found")
    finally:
        connection.close()

# Update (PUT) endpoint
@app.put("/movies/{movie_id}", response_model=Movie)
async def update_movie(movie_id: int, movie_update: MovieUpdate):
    update_data = {k: v for k, v in movie_update.dict().items() if v is not None}
    if update_data:
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                sql = "UPDATE Movies SET " + ", ".join(f"{k} = %s" for k in update_data.keys()) + " WHERE movie_id = %s"
                cursor.execute(sql, (*update_data.values(), movie_id))
                connection.commit()
                updated_movie = await read_movie(movie_id)
                return updated_movie
        finally:
            connection.close()
    raise HTTPException(status_code=404, detail="Movie not found")

# Delete (DELETE) endpoint
@app.delete("/movies/{movie_id}", response_model=dict)
async def delete_movie(movie_id: int):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "DELETE FROM Movies WHERE movie_id = %s"
            cursor.execute(sql, (movie_id,))
            connection.commit()
            if cursor.rowcount:
                return {"status": "success", "message": "Movie deleted"}
            raise HTTPException(status_code=404, detail="Movie not found")
    finally:
        connection.close()

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)