import json
import logging
from pathlib import Path

class JsonConfig:
    def __init__(self, filePath:str):
        try:
            # Fix: Clean surrounding quotes and spaces before opening
            clean_path = str(filePath).strip(' "')
            with open(clean_path, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file not found at {filePath}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON in {filePath}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error parsing JSON in {filePath}: {e}")
            raise        

    def populateToConfigMap(self, configMap, sectionName:str):
        keys = list(self.config.keys())

        # Ensure the 'PARAMETER' section exists in the ConfigParser object
        if not configMap.has_section(sectionName):
            configMap.add_section(sectionName)

        for key in keys:
            configMap.set(sectionName, key, str(self.config.get(key)))