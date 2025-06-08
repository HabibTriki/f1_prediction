import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_postgresql_connection():
    """Test direct PostgreSQL connection"""
    try:
        # Connection parameters
        host = "localhost"
        port = 5432
        database = "formula1"
        user = os.getenv("POSTGRES_USER", "f1user")
        password = os.getenv("POSTGRES_PASSWORD", "f1password")
        
        print(f"Testing connection to PostgreSQL:")
        print(f"Host: {host}:{port}")
        print(f"Database: {database}")
        print(f"User: {user}")
        print(f"Password: {'*' * len(password)}")
        
        # Create connection
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✓ Connection successful!")
        print(f"PostgreSQL version: {version[0]}")
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'historical_avg_laps'
            );
        """)
        table_exists = cursor.fetchone()[0]
        print(f"Table 'historical_avg_laps' exists: {table_exists}")
        
        # If table doesn't exist, create it
        if not table_exists:
            print("Creating table 'historical_avg_laps'...")
            cursor.execute("""
                CREATE TABLE historical_avg_laps (
                    driver VARCHAR(255) NOT NULL,
                    avg_lap_time REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (driver)
                );
            """)
            conn.commit()
            print("✓ Table created successfully!")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_postgresql_connection()
    if not success:
        print("\nTroubleshooting steps:")
        print("1. Check if PostgreSQL container is running: docker-compose ps")
        print("2. Check PostgreSQL logs: docker-compose logs postgres")
        print("3. Restart containers: docker-compose restart postgres")