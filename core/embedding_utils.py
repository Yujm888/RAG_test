# core/embedding_utils.py
from openai import OpenAI
import numpy as np
import config

logger = config.logger

try:
    client = OpenAI(
        api_key=config.API_KEY,
        base_url=config.BASE_URL,
    )
    logger.info("OpenAI 客户端初始化成功。")
except Exception as e:
    logger.error(f"OpenAI 客户端初始化失败: {e}")
    client = None


def get_embedding(text_chunk: str) -> np.ndarray | None:
    """
    获取单个文本块的向量。
    """
    if client is None:
        logger.error("OpenAI 客户端未初始化，无法获取 embedding。")
        return None
    text_chunk = text_chunk.replace("\n", " ")
    try:
        response = client.embeddings.create(model=config.EMBEDDING_MODEL_NAME, input=[text_chunk])
        return np.array(response.data[0].embedding)
    except Exception as e:
        logger.error(f"获取 embedding 时出错: {e}")
        return None