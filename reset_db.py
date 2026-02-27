from sqlalchemy import text
from src.db.session import engine

def reset_database():
    print("Connecting to database..")
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
        conn.commit()
    print("Database is reset")

if __name__ == "__main__":
    reset_database()