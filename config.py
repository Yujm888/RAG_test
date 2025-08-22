# config.py

import os
import logging
from dotenv import load_dotenv

# --- 核心修正：构建到项目根目录的绝对路径 ---
# 首先，我们确定项目的根目录
# __file__ 是当前文件 (config.py) 的路径
# os.path.dirname(__file__) 就是 config.py 所在的目录，也就是项目根目录
PROJECT_ROOT = os.path.dirname(__file__)

# --- 1. 加载 .env 文件中的环境变量 ---
# 我们也使用绝对路径来加载 .env 文件，更健壮
dotenv_path = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path=dotenv_path)


# --- 2. 从环境变量中获取配置信息 ---
# API 配置
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

# --- Oracle DB 配置 ---
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")

# --- 知识库路径配置 ---
# 源文档所在的文件夹
SOURCE_DOCS_DIR = os.path.join(PROJECT_ROOT, "knowledge_base/source_documents")

# 生成的索引和数据文件存放的文件夹
GENERATED_DATA_DIR = os.path.join(PROJECT_ROOT, "knowledge_base/generated")

# 统一的知识库文件名
KB_INDEX_FILE = "knowledge.index"
KB_CHUNKS_FILE = "knowledge.json"

# --- 拼接完整路径 ---
# os.path.join 会根据你的操作系统自动使用正确的路径分隔符
INDEX_FILE_PATH = os.path.join(GENERATED_DATA_DIR, KB_INDEX_FILE)
CHUNKS_FILE_PATH = os.path.join(GENERATED_DATA_DIR, KB_CHUNKS_FILE)

# 3. 配置日志系统 (专业版)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- 使用绝对路径来指定日志文件 ---
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, 'app.log')
file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)

# --- 创建控制台处理器：只在控制台显示警告和错误 ---
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.WARNING)

# --- 创建日志格式 ---
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 为处理器设置格式 ---
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# --- 为根logger添加处理器 ---
if logger.hasHandlers():
    logger.handlers.clear()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# --- 4. 启动时检查关键配置 ---
if not API_KEY or not BASE_URL:
    logger.error("关键配置缺失: 环境变量 API_KEY 或 BASE_URL 未设置。请检查 .env 文件。")
    raise ValueError("API_KEY 或 BASE_URL 未设置，程序无法启动。")


if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN]):
    logger.warning("数据库配置不完整: 环境变量 ORACLE_USER, ORACLE_PASSWORD, 或 ORACLE_DSN 未全部设置。Text-to-SQL (Oracle) 功能可能无法使用。")

logger.info("配置加载成功，日志系统已初始化。")