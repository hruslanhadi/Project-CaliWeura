"""
PostgreSQL Database Connection Module
Connects to a local PostgreSQL database using psycopg2
"""

import psycopg2
from psycopg2 import sql, Error
from typing import Optional, List


class DatabaseConnection:
    """Class to handle PostgreSQL database connections and operations"""
    
    def __init__(self, host: str = 'localhost', 
                 database: str = 'your_database',
                 user: str = 'postgres',
                 password: str = 'your_password',
                 port: int = 5432):
        """
        Initialize database connection parameters
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Successfully connected to PostgreSQL database")
            return True
        except Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute a SQL query (INSERT, UPDATE, DELETE)"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            print(f"Query executed. Rows affected: {self.cursor.rowcount}")
            return True
        except Error as e:
            self.connection.rollback()
            print(f"Error executing query: {e}")
            return False
    
    def fetch_data(self, query: str, params: Optional[tuple] = None) -> Optional[List]:
        """Fetch data from database (SELECT)"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            result = self.cursor.fetchall()
            print(f"Fetched {len(result)} rows")
            return result
        except Error as e:
            print(f"Error fetching data: {e}")
            return None
    
    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[tuple]:
        """Fetch a single row from database"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result
        except Error as e:
            print(f"Error fetching data: {e}")
            return None


# Example usage
if __name__ == "__main__":
    db = DatabaseConnection(
        host='localhost',
        database='your_database_name',  # Replace with your database name
        user='postgres',                # Replace with your username
        password='your_password',       # Replace with your password
        port=5432
    )
    
    if db.connect():
        try:
            # Example: Create a simple table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            db.execute_query(create_table_query)
            
            # Example: Insert data
            insert_query = "INSERT INTO users (name, email) VALUES (%s, %s)"
            db.execute_query(insert_query, ("John Doe", "john@example.com"))
            
            # Example: Fetch data
            select_query = "SELECT * FROM users;"
            data = db.fetch_data(select_query)
            if data:
                for row in data:
                    print(row)
        finally:
            db.disconnect()
