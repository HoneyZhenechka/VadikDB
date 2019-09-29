import os
from pathlib import Path


class DataBaseManager:
    current_directory = ""

    def __init__(self):
        current_directory = Path.cwd()
        db_directory = current_directory / "db/"
        if not db_directory.exists():
            os.makedirs(db_directory)
        current_directory = db_directory

