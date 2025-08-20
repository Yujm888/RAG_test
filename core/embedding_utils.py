# core/embedding_utils.py
from openai import OpenAI
import numpy as np
import config  # 导入我们的全局配置

# 从配置中获取日志记录器
logger = config.logger

# --- 1. 初始化 OpenAI 客户端 ---
# 这个客户端实例将在整个应用中被复用
try:
    client = OpenAI(
        api_key=config.API_KEY,
        base_url=config.BASE_URL,
    )
    logger.info("OpenAI 客户端初始化成功。")
except Exception as e:
    logger.error(f"OpenAI 客户端初始化失败: {e}")
    client = None

# --- 2. 封装获取 embedding 的函数 ---
def get_embedding(text_chunk: str) -> np.ndarray | None:
    """
    获取单个文本块的向量。
    :param text_chunk: 需要向量化的文本字符串。
    :return: Numpy 向量数组，如果失败则返回 None。
    """
    if client is None:
        logger.error("OpenAI 客户端未初始化，无法获取 embedding。")
        return None
    # 替换掉换行符，某些模型的 embedding 接口可能不喜欢
    text_chunk = text_chunk.replace("\n", " ")
    try:
        response = client.embeddings.create(model="text-embedding-v4", input=[text_chunk])
        return np.array(response.data[0].embedding)
    except Exception as e:
        logger.error(f"获取 embedding 时出错: {e}")
        return None