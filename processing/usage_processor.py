import asyncio
import duckdb
import pandas as pd
import random

async def fetch_telemetry_batch(batch_id: int):
    """Simulate async fetching of vehicle telemetry data."""
    await asyncio.sleep(random.uniform(0.1, 0.5))  # Simulate network latency
    # Generate mock telemetry data
    return pd.DataFrame({
        'vehicle_id': [f"V-{random.randint(100, 150)}" for _ in range(1000)],
        'ping_latency_ms': [random.uniform(10.0, 500.0) for _ in range(1000)],
        'batch_id': batch_id
    })

async def main():
    print("Fetching batches concurrently...")
    # AsyncIO to fetch 10 batches concurrently
    tasks = [fetch_telemetry_batch(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    
    # Combine all pandas dataframes
    all_telemetry = pd.concat(results)

    print("Processing with DuckDB...")
    # Initialize DuckDB in-memory
    con = duckdb.connect(database=':memory:')
    
    # Register the pandas dataframe as a DuckDB table
    con.register('raw_telemetry', all_telemetry)
    
    # Aggregate data using DuckDB
    aggregated_df = con.execute("""
        SELECT 
            vehicle_id, 
            COUNT(*) as total_pings,
            AVG(ping_latency_ms) as avg_latency
        FROM raw_telemetry
        GROUP BY vehicle_id
        HAVING avg_latency > 50.0
    """).df()

    # Write output to Parquet
    con.execute("COPY (SELECT * FROM aggregated_df) TO 'processed_telemetry.parquet' (FORMAT PARQUET)")
    print("Exported high-latency vehicle aggregates to Parquet.")

if __name__ == "__main__":
    asyncio.run(main())