# config.py

import os
import logging
from dotenv import load_dotenv

# --- 构建到项目根目录的绝对路径 ---
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
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME")
ORACLE_SID = os.getenv("ORACLE_SID")

# --- 新增：模型与应用行为配置 ---
# 大语言模型名称
LLM_MODEL_NAME = "qwen-plus"
# Embedding 模型名称
EMBEDDING_MODEL_NAME = "text-embedding-v4"
# Reranker 模型名称
RERANKER_MODEL_NAME = "BAAI/bge-reranker-base"

# RAG 流程参数
# Reranker 模型能接受的最大序列长度
RERANKER_MAX_LENGTH = 512
# 检索阶段返回的文档块数量
SEARCH_TOP_K = 5
# RAG 上下文允许的最大字符数
MAX_CONTEXT_CHARS = 8000

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


if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST, ORACLE_PORT]) or not (ORACLE_SERVICE_NAME or ORACLE_SID):
    logger.warning("数据库配置不完整: 请检查 .env 文件中的 ORACLE_USER, PASSWORD, HOST, PORT 以及 SERVICE_NAME 或 SID。")

logger.info("配置加载成功，日志系统已初始化。")