from animal_etl.etl.utils import *
logger = setup_logger("Extract")

def extract_animals(base_url="http://localhost:3123"):
    session = get_session()
    page = 1
    total_fetched = 0
    
    logger.info("Starting extraction...")
    
    while True:
        url = f"{base_url}/animals/v1/animals"
        try:
            logger.debug(f"Fetching page {page}")
            response = session.get(url, params={"page": page})
            response.raise_for_status()
            print(f"Fetched page {page} of {len(response.json())}")
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                logger.info("Extraction complete.")
                break
                
            for animal in items:
                print(f"Extracting animal: {animal}")
                yield animal
                total_fetched += 1
                
            logger.info(f"Page {page} extracted.")
            page += 1
            
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            raise

    logger.info(f"Extraction complete. Total: {total_fetched}")
