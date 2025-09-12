import logging
from pathlib import Path
from typing import Optional, List

import pandas as pd

from src.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Responsible for saving each transformed DataFrame into a CSV in the output folder.
    The output_path passed in main() is respected here.
    """

    def __init__(self, output_path: Optional[str] = None):
        if output_path:
            self.output_path = Path(output_path)
        else:
            self.output_path = Path("data/output")
        ensure_directory(self.output_path)
        logger.info(f"Output directory set to: {self.output_path.resolve()}")

    def save_to_csv(self, df: pd.DataFrame, entity_name: str, field_order: List[str]) -> None:
        """
        Write df to <output_path>/<EntityName>.csv (no index). Overwrites if it already exists.
        """
        try:
            output_file = self.output_path / f"{entity_name}.csv"
            # Reorder columns to match the YAML file
            df = df[field_order]
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {entity_name}.csv ({len(df)} rows) to {output_file}")
        except Exception as ex:
            logger.error(f"Failed to save {entity_name} to CSV: {ex}")
            raise