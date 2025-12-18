import sys
import os

# Ensure the current directory is in the path
sys.path.append(os.getcwd())

from animal_etl.etl.extract import extract_animals
from animal_etl.etl.transform import transform_animal
from animal_etl.etl.load import load_animals
from animal_etl.etl.utils import setup_logger

logger = setup_logger("Main")

def main():
    logger.info("Starting ETL process...")
    
    try:
        # Extracting animals
        animals_generator = extract_animals()
        
        batch = []
        total_processed = 0
        
        for animal in animals_generator:
            transformed = transform_animal(animal)
            batch.append(transformed)
            
            # Load in batches of 100
            if len(batch) >= 100:
                load_animals(batch)
                total_processed += len(batch)
                batch = []
                
        # Load remaining
        if batch:
            load_animals(batch)
            total_processed += len(batch)
            
        logger.info(f"ETL Complete. Processed {total_processed} animals.")
    except Exception as e:
        logger.error(f"ETL Failed: {e}")
        raise

if __name__ == "__main__":
    main()
