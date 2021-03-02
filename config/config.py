import os

import toml

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))

CONFIG_NAME = os.environ.get("CONFIG_NAME")
if CONFIG_NAME is not None:
    CONFIG_PATH = os.path.abspath(os.path.join(BASE_DIR, f"{CONFIG_NAME}.toml"))
    if os.path.exists(CONFIG_PATH):
        CONFIG = toml.load(CONFIG_PATH)
    else:
        raise FileNotFoundError(f"配置文件 {CONFIG_PATH} 未找到")
else:
    print("Warning: 未加载配置文件,环境变量CONFIG_NAME未设置")
