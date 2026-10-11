import pathlib

from starlette.config import Config

ROOT = pathlib.Path(__file__).resolve().parent.parent  # app/
BASE_DIR = ROOT.parent  # ./

env_path = BASE_DIR / ".env"
if env_path.exists():
    starlette_config = Config(env_path)
else:
    starlette_config = Config()

# Alias for backward compatibility if needed, but we should migrate
config = starlette_config
