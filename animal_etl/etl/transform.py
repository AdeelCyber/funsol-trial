from datetime import datetime, timezone
from animal_etl.etl.utils import *

logger = setup_logger("Transform")

def transform_animal(animal):
    """
    This function copies the animal and run the requirements. ie, convert friends_dict to array seperated by ","
    """
    transformed = animal.copy()
    
    friends_raw = transformed.get("friends")
    print("raw",transformed)
    if isinstance(friends_raw, str):
        if friends_raw:
            transformed["friends"] = [f.strip() for f in friends_raw.split(",")]
        else:
            transformed["friends"] = []
    elif friends_raw is None:
        transformed["friends"] = []
    
    print(transformed)
    born_at_raw = transformed.get("born_at")
    born_at_raw = transformed.get("born_at")
    if born_at_raw is not None:
        try:
            # Handle milliseconds
            dt = datetime.fromtimestamp(born_at_raw / 1000.0, tz=timezone.utc)
            transformed["born_at"] = dt.isoformat()
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to transform 'born_at' for animal {transformed.get('id')}: {born_at_raw} - {e}")
            # Keep original value if transformation fails, or set to None if strict? 
            # Requirement says "if populated, must be translated". If fails, likely better to keep or log.
    print(transformed)
    return transformed
