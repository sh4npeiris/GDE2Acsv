import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Union

import pandas as pd
import yaml

from src.etl.extractor import DataExtractor
from src.etl.loader import DataLoader
from src.etl.transformer import DataTransformer
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract_required_files_from_config(mappings: Dict[str, Dict], global_config: Dict[str, Any]) -> List[str]:
    """
    Extract all required files from the configuration, handling both list and dict formats
    """
    required_files = set()
    
    # Extract from entity mappings
    for entity_config in mappings.values():
        source_config = entity_config.get("source_files", {})
        if isinstance(source_config, dict):
            # New format: {"role": "filename"}
            required_files.update(source_config.values())
        elif isinstance(source_config, list):
            if all(isinstance(item, dict) for item in source_config):
                # New format with roles: [{"file": "filename", "role": "role"}]
                for item in source_config:
                    if "file" in item:
                        required_files.add(item["file"])
            else:
                # Legacy format: ["filename1", "filename2"]
                required_files.update(source_config)
    
    # Extract from global config
    sy_sources_config = global_config.get("school_year_sources", {})
    if isinstance(sy_sources_config, dict):
        required_files.update(sy_sources_config.values())
    elif isinstance(sy_sources_config, list):
        required_files.update(sy_sources_config)
    
    return list(required_files)

def get_source_files_list(source_config: Union[Dict, List]) -> List[str]:
    """
    Convert source configuration to a list of filenames
    """
    if isinstance(source_config, dict):
        # New format: {"role": "filename"}
        return list(source_config.values())
    elif isinstance(source_config, list):
        if all(isinstance(item, dict) for item in source_config):
            # New format with roles: [{"file": "filename", "role": "role"}]
            return [item["file"] for item in source_config if "file" in item]
        else:
            # Legacy format: ["filename1", "filename2"]
            return source_config
    return []

def main(sis_type: str, input_path: str, output_path: str) -> None:
    """
    Entry point for the ETL CLI.
    """
    input_dir = Path(input_path)
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Input path is not a directory: {input_dir}")
        sys.exit(1)
    logger.info(f"Input directory: {input_dir.resolve()}")

    mapping_file = Path("config/mappings") / f"{sis_type}_mapping.yaml"
    if not mapping_file.exists():
        logger.error(f"Mapping file not found: {mapping_file}")
        sys.exit(1)

    with open(mapping_file, "r") as mf:
        full_mapping = yaml.safe_load(mf)
        mappings: Dict[str, Dict] = full_mapping.get("mappings", {})
        global_config: Dict[str, Any] = full_mapping.get("global_config", {})

    extractor = DataExtractor(input_path)
    transformer = DataTransformer()
    loader = DataLoader(output_path)

    # Get all required files from configuration
    required_files = extract_required_files_from_config(mappings, global_config)
    logger.info(f"Required files: {required_files}")

    raw_data = extractor.load_data(required_files)

    # Determine school year
    sy_sources_config = global_config.get("school_year_sources", {})
    sy = transformer.determine_school_year(raw_data, sy_sources_config)
    transformer.set_school_year(sy)
    logger.info(f"Using school year {sy}, academic start={transformer.academic_start}, end={transformer.academic_end}")

    for entity_name in ("Students", "Staff", "Family", "Classes", "Enrollments"):
        entity_cfg = mappings.get(entity_name, {})
        source_config = entity_cfg.get("source_files", {})
        
        if not source_config:
            logger.warning(f"No source_files for entity '{entity_name}' in the mapping; skipping.")
            continue

        # Get the list of source files for this entity
        source_files = get_source_files_list(source_config)
        if not source_files:
            logger.warning(f"No valid source files for entity '{entity_name}'; skipping.")
            continue

        # For the transformer, we need to pass the first file as df parameter
        # but the transformer will handle the complex logic internally
        primary_source = source_files[0]
        primary_df = raw_data.get(primary_source, pd.DataFrame())
        
        if primary_df.empty:
            logger.warning(f"Primary source file '{primary_source}' is empty for '{entity_name}'; skipping.")
            continue

        # The transformer now handles all logic, including complex joins, internally.
        transformed = transformer.transform(primary_df, entity_cfg, entity_name, raw_data, global_config)
        
        if transformed.empty:
            logger.warning(f"No data transformed for entity '{entity_name}'; skipping.")
            continue
        
        field_order = list(entity_cfg.get("field_map", {}).keys())
        loader.save_to_csv(transformed, entity_name, field_order)

    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SIS Data ETL Tool for myBlueprint - SpacesEDU"
    )
    parser.add_argument("--sis", required=True, help="SIS type (e.g., myedbc)")
    parser.add_argument("--input", required=True, help="Path to input GDE files")
    parser.add_argument("--output", default="data/output", help="Output path for CSV files")
    args = parser.parse_args()
    main(args.sis.lower(), args.input, args.output)