import yaml
import logging
from pathlib import Path
import shutil
import os
import json
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class utils:
    """Class with utility methods"""

    @staticmethod
    def load_config():
        """Loads configuration from config.yaml"""
        for path in (Path("/app/config.yaml"), Path("config.yaml")):
            if path.exists():
                logger.info(f"Loading configuration from {path}")
                with path.open("r") as file:
                    config = yaml.safe_load(file)
                logger.info("Config loaded successfully.")
                break  # Stop searching once the file is found
        else:
            raise FileNotFoundError("No config file found in /app/config.yaml or ./config.yaml")

        return config
        
    @staticmethod
    def get_structure(base_dir="etl"):
        """
        Scans the base folder and returns the structure in text format.
        - Ignores folders '__pycache__', 'packages', 'history', and 'storage'.
        - Shows 'logs/' but not its content.
        - Formats the output with 'tree' like lines.
        """

        structure = ["etl/"]  # The root is always 'etl'

        def traverse_directory(path, prefix=""):
            """Traverses the folder and generates the 'tree' formatted structure."""
            elements = sorted(os.listdir(path))
            
            # Filter folders to be ignored
            elements = [e for e in elements if e not in ("__pycache__", "packages", "history", "storage")]

            for i, element in enumerate(elements):
                full_path = os.path.join(path, element)
                is_last = (i == len(elements) - 1)
                new_prefix = prefix + ("└── " if is_last else "├── ")

                if os.path.isdir(full_path):
                    if element.lower() == "logs":  # Only show the 'logs' folder, no content
                        structure.append(prefix + ("└── " if is_last else "├── ") + element + "/")
                        continue
                    structure.append(new_prefix + element + "/")
                    traverse_directory(full_path, prefix + ("    " if is_last else "│    "))
                else:
                    structure.append(new_prefix + element)

        traverse_directory(base_dir)
        return "\n".join(structure)

    @staticmethod
    def save_structure_txt(base_dir="etl", txt_path="ETL_architecture.txt"):
        """
        Saves the directory structure to a text file and prints it to the console.
        """
        text_structure = utils.get_structure(base_dir)
        
        # Print to console
        print("\n" + text_structure + "\n")
        
        # Save to file
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text_structure)
        print(f"Structure saved to {txt_path}")

    @staticmethod
    def organize_project(base_dir="etl"):
        """
        Executes the complete process:
        1. Generates the structure in text.
        2. Prints it to the console.
        3. Saves it to 'etl_architecture.txt'.
        """
        utils.save_structure_txt(base_dir)

    def restart_container(container_name, context):
        """
        Restarts a specified Docker container.

        Args:
            container_name (str): The name of the container to restart.
            context: An object with a 'log' attribute for logging messages.
        """
        try:
            result = subprocess.run(
                ["docker", "restart", container_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                context.log.info(f"Container {container_name} restarted successfully.")
            else:
                context.log.error(f"Failed to restart {container_name}: {result.stderr}")
        except Exception as e:
            context.log.error(f"Exception while restarting container: {e}")


if __name__ == "__main__":
    utils.organize_project()