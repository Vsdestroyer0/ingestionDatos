import os
import re
from sqlalchemy import create_engine, text

# Database connection details
DB_URL = "postgresql+pg8000://admin:password@localhost:5432/steam_analytics"
SCHEMA_FILE = "Juegos.sql"

def init_database():
    print(f"Connecting to database: {DB_URL}")
    engine = create_engine(DB_URL)
    
    if not os.path.exists(SCHEMA_FILE):
        print(f"Error: Schema file '{SCHEMA_FILE}' not found.")
        return
        
    print(f"Reading schema from {SCHEMA_FILE}...")
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        sql_content = f.read()
        
    # Remove SQL comments
    sql_clean = re.sub(r'--.*', '', sql_content)
    
    # Split by semicolon to get individual commands
    statements = [stmt.strip() for stmt in sql_clean.split(';') if stmt.strip()]
    
    print("Executing database schema initialization...")
    # Execute each statement in its own connection block (separate transactions)
    for statement in statements:
        try:
            with engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
            print(f"Statement executed successfully.")
        except Exception as e:
            # Check if it's an "already exists" error
            error_str = str(e)
            if "already exists" in error_str:
                print(f"Notice: Table or relation already exists, skipping.")
            else:
                print(f"Warning: Failed to execute statement. Details: {e}")

    print("Database initialization complete.")

if __name__ == "__main__":
    init_database()
