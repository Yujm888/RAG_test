# config.py

import os
import logging
from dotenv import load_dotenv


# --- 1. 加载 .env 文件中的环境变量 ---
load_dotenv()


# --- 2. 从环境变量中获取配置信息 ---
# API 配置
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

# --- 知识库路径配置 ---
# 源文档所在的文件夹
SOURCE_DOCS_DIR = "knowledge_base/source_documents"

# 生成的索引和数据文件存放的文件夹
GENERATED_DATA_DIR = "knowledge_base/generated"

# 统一的知识库文件名
KB_INDEX_FILE = "knowledge.index"
KB_CHUNKS_FILE = "knowledge.json"

# --- 拼接完整路径 ---
# os.path.join 会根据你的操作系统自动使用正确的路径分隔符
INDEX_FILE_PATH = os.path.join(GENERATED_DATA_DIR, KB_INDEX_FILE)
CHUNKS_FILE_PATH = os.path.join(GENERATED_DATA_DIR, KB_CHUNKS_FILE)

# 3. 配置日志系统 (专业版)
# 不要使用 basicConfig，因为它是一次性配置。我们手动配置更灵活。
logger = logging.getLogger()  # 获取根logger
logger.setLevel(logging.INFO)  # 设置根logger的级别为INFO，这是总开关

# --- 创建文件处理器：记录所有日志到 app.log 文件 ---
# 'a' 模式表示追加写入, a for append
# encoding='utf-8' 确保中文字符不会乱码
file_handler = logging.FileHandler("app.log", mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)  # 文件处理器记录INFO及以上所有级别

# --- 创建控制台处理器：只在控制台显示警告和错误 ---
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.WARNING)  # 控制台处理器只显示WARNING及以上级别

# --- 创建日志格式 ---
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 为处理器设置格式 ---
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# --- 为根logger添加处理器 ---
# 清除可能存在的旧处理器，防止重复输出
if logger.hasHandlers():
    logger.handlers.clear()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# --- 4. 启动时检查关键配置，确保程序能正常运行 ---
if not API_KEY or not BASE_URL:
    logger.error("关键配置缺失: 环境变量 API_KEY 或 BASE_URL 未设置。请检查 .env 文件。")
    raise ValueError("API_KEY 或 BASE_URL 未设置，程序无法启动。")

logger.info("配置加载成功，日志系统已初始化。")