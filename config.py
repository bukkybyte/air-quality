from pydantic_settings import BaseSettings  # Import from pydantic-settings
import os


def read_full_path(filename: str = ".env"):
    """ Uses os to return the correct path of the .env file."""
    absolute_path = os.path.abspath(__file__)
    directory_name = os.path.dirname(absolute_path)
    full_path = os.path.join(directory_name, filename)
    return full_path


# Get the full path of the .env file
env_file_path = read_full_path(".env")


class Config(BaseSettings):
    # Define the settings as class attributes
    db_name: str

    class Config:
        # This tells Pydantic to load values from a .env file
        env_file = env_file_path


# Instance of `settings`
settings = Config()