from typing import Optional
from datetime import datetime
import sqlite3
from pydantic import BaseModel, Field, field_validator

import csv
from io import StringIO
from typing import Union


class AttendanceRecord(BaseModel):
    """
    Pydantic model for the 'in' table schema
    """
    roll_num: str = Field(..., description="Student roll number")
    name: str = Field(..., description="Student name")
    department: str = Field(..., description="Department name")
    time: datetime = Field(default_factory=datetime.now, description="Timestamp of the record")

    @field_validator('roll_num')
    def validate_roll_num(cls, v):
        if not v.strip():
            raise ValueError("roll_num cannot be empty")
        return v

    @field_validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()

    @field_validator('department')
    def validate_department(cls, v):
        if not v.strip():
            raise ValueError("department cannot be empty")
        return v.strip()

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.strftime('%Y-%m-%d %H:%M:%S')
        }

def store_in_table(tablename: str):
    def store_data(data: AttendanceRecord | dict, db_connection: sqlite3.Connection) -> bool:
        try:
            # Convert dict to Pydantic model if necessary
            if isinstance(data, dict):
                record = AttendanceRecord(**data)
            else:
                record = data

            cursor = db_connection.cursor()

            print(data)
            
            # Prepare the SQL query
            query = """
            INSERT INTO """+ f"'{tablename}'" +""" (roll_num, name, department, time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT do nothing
            """
            
            # Execute the query with parameters
            cursor.execute(query, (
                record.roll_num,
                record.name,
                record.department,
                record.time.strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            # Commit the transaction
            db_connection.commit()
            
            return True
            
        except sqlite3.Error as e:
            db_connection.rollback()
            raise sqlite3.Error(f"Database error occurred: {str(e)}")
        
        finally:
            cursor.close()

    return store_data



def get_table_as_csv_buffer(connection: sqlite3.Connection, table_name: str = 'in') -> StringIO:
    """
    Retrieves data from the specified SQLite table and returns it as a CSV buffer.
    
    Args:
        connection: Either an SQLite connection object or a string path to the database
        table_name: Name of the table to export (defaults to 'in')
    
    Returns:
        StringIO: A buffer containing the CSV data
        
    Raises:
        sqlite3.Error: If there's an issue with the database connection or query
    """
    # Create a buffer to store CSV data
    csv_buffer = StringIO()
    cursor = connection.cursor()
    
    # Get all data from the table
    cursor.execute(f"SELECT roll_num, name, department, time FROM '{table_name}'")
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Create CSV writer and write data
    writer = csv.writer(csv_buffer)
    writer.writerow(column_names)  # Write headers
    writer.writerows(rows)         # Write data rows
    
    # Reset buffer position to start
    csv_buffer.seek(0)
    return csv_buffer
        
