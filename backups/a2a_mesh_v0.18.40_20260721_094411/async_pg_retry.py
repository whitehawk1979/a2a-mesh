import asyncio
import logging
from typing import List, Dict, Any, Optional
import asyncpg

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def execute_query_with_retry(
    conn_params: Dict[str, Any], 
    query: str, 
    params: tuple = (), 
    max_attempts: int = 3
) -> List[Dict[str, Any]]:
    """
    Connects to PostgreSQL, executes a parameterized query with exponential backoff retries,
    and returns results as a list of dictionaries.
    """
    attempt = 0
    while attempt < max_attempts:
        conn = None
        try:
            # 1. Connection Phase
            try:
                conn = await asyncpg.connect(**conn_params)
            except (asyncpg.PostgresError, OSError) as conn_err:
                logger.error(f"Connection attempt {attempt + 1} failed: {conn_err}")
                raise ConnectionError(f"Could not connect to database: {conn_err}")

            # 2. Execution Phase
            try:
                # fetch returns a list of Record objects, which behave like dicts
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
            except asyncpg.PostgresError as query_err:
                logger.error(f"Query execution failed: {query_err}")
                # Query errors (like syntax or constraint violations) usually shouldn't be retried
                # unless they are transient (e.g., serialization failures in high isolation levels).
                # For this general implementation, we treat them as fatal for the request.
                raise query_err
            finally:
                if conn:
                    await conn.close()

        except ConnectionError as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error("Max connection attempts reached.")
                raise e
            
            delay = 2 ** attempt  # Exponential backoff: 2, 4, 8...
            logger.info(f"Retrying connection in {delay} seconds...")
            await asyncio.sleep(delay)
            
        except Exception as e:
            # For non-connection errors (like query errors), we fail immediately
            logger.error(f"An unexpected error occurred: {e}")
            raise e

# Example Usage
if __name__ == "__main__":
    # Configuration for the DB connection
    db_config = {
        "user": "postgres",
        "password": "password",
        "database": "test_db",
        "host": "127.0.0.1",
        "port": 5432
    }

    async def main():
        try:
            sql = "SELECT * FROM users WHERE status = $1"
            arguments = ("active",)
            results = await execute_query_with_retry(db_config, sql, arguments)
            print(f"Results: {results}")
        except Exception as e:
            print(f"Final failure: {e}")

    asyncio.run(main())
