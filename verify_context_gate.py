
import asyncio
import asyncpg
import json
import os
import sys

# Add the core directory to sys.path
sys.path.append('/Users/zsolt/.hermes/scripts/a2a_mesh')

from core.context_gate import get_context_status

async def main():
    print("Connecting to PG...")
    try:
        # Connection details from prompt
        pool = await asyncpg.create_pool(
            host='192.168.1.30',
            port=5432,
            user='nova',
            password='nova_agent_2026',
            database='agent_memory'
        )
        
        print("Fetching context status...")
        # We pass None for node since we are running standalone, but the code should handle it
        status = await get_context_status(pg_pool=pool, node=None)
        
        print(json.dumps(status, indent=2))
        
        # Verification: check for 5 nodes
        agents = [a['agent'] for a in status.get('agents', [])]
        print(f"\nNodes found: {agents}")
        print(f"Count: {len(agents)}")
        
        expected = {'nova', 'morzsa', 'runa', 'tor', 'mano'}
        found = set(agents)
        missing = expected - found
        
        if not missing:
            print("SUCCESS: All expected nodes found.")
        else:
            print(f"FAILURE: Missing nodes: {missing}")
            
        await pool.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    asyncio.run(main())
