from animal_etl.etl.utils import *

logger = setup_logger("Load")

def load_animals(animals, base_url="http://localhost:3123"):
    """
    Loads animals paginated.
    """
    if not animals:
        return

    session = get_session()
    url = f"{base_url}/animals/v1/home"
    
    batch_size = 100
    for i in range(0, len(animals), batch_size):
        batch = animals[i:i + batch_size]
        print("batch",batch)

        logger.info(f"Uploading batch of {len(batch)} animals...")
        
        try:
            response = session.post(url, json=batch)
            response.raise_for_status()
            logger.info("Batch upload successful.")
        except Exception as e:
            logger.error(f"Failed to transfer batch: {e}")
            raise
