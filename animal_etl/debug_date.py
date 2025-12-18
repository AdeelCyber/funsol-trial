from datetime import datetime, timezone

ts = 945119028291
try:
    dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
    print(f"Input: {ts}")
    print(f"Converted: {dt}")
    print(f"ISO: {dt.isoformat()}")
except Exception as e:
    print(f"Error: {e}")
