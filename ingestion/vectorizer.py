# ingestion/vectorizer.py
import time
import config
from core.embedding_utils import get_embedding

logger = config.logger

def get_all_embeddings(chunks):
    """获取所有文本块的向量"""
    logger.info("开始批量向量化所有文本块...")
    valid_chunks = []
    all_vectors = []
    start_time = time.time()
    for i, chunk_dict in enumerate(chunks):
        if (i + 1) % 10 == 0:
            logger.info(f"正在处理第 {i + 1}/{len(chunks)} 个文本块...")
        text_to_embed = chunk_dict.get("text", "")      #.get是字典中安全获取键的方法，如果该键不存在，它会返回默认值""
        if not text_to_embed or not text_to_embed.strip():      #对于空白文本块，跳过
            continue
        vector = get_embedding(text_to_embed)
        if vector is not None:
            valid_chunks.append(chunk_dict)
            all_vectors.append(vector)      #保证添加的文本块与向量一一对应
    end_time = time.time()
    logger.info(f"批量向量化完成，共{len(all_vectors)}个向量，耗时 {end_time - start_time:.2f} 秒。")        #.2f将浮点数格式转化为保留两位小数
    return valid_chunks, all_vectors
