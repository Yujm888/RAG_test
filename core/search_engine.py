# core/search_engine.py
import faiss
import json
import numpy as np
import os
from rank_bm25 import BM25Okapi
import jieba
from sentence_transformers import CrossEncoder
import config
from core.embedding_utils import get_embedding

logger = config.logger


class SearchEngine:
    """封装了 FAISS, BM25 和 Re-ranking 的搜索引擎。"""


    # 构造方法
    def __init__(self, index_path: str, chunks_path: str):
        logger.info("--- 正在初始化搜索引擎 ---")
        self.faiss_index = None         #存储FAISS索引
        self.chunks_with_metadata = []  #存储知识块列表
        self.plain_chunks = []          #存储文本列表
        self.bm25_index = None          #存储BM25索引
        self.reranker = None            #存储重排序模型

        self.faiss_index, self.chunks_with_metadata = self._load_knowledge_base(index_path, chunks_path)

        if self.faiss_index:
            self.plain_chunks = [item['text'] for item in self.chunks_with_metadata]#列表推导式
            self.bm25_index = self._build_bm25_index(self.plain_chunks)
            self._load_reranker_model()

            if self.bm25_index and self.reranker:
                logger.info("--- 搜索引擎初始化成功 (FAISS + BM25 + Re-ranker) ---")
            else:
                logger.warning("--- 搜索引擎初始化不完整 (请检查BM25或Re-ranker) ---")
        else:
            logger.error("搜索引擎初始化失败，因为无法加载知识库。")


    #内部方法
    def _load_reranker_model(self):
        """加载重排序模型。"""
        try:
            logger.info(f"正在加载重排序模型 ({config.RERANKER_MODEL_NAME})...")
            self.reranker = CrossEncoder(config.RERANKER_MODEL_NAME, max_length=config.RERANKER_MAX_LENGTH)#加载模型
            logger.info("重排序模型加载成功。")
        except Exception as e:
            logger.error(f"加载重排序模型失败: {e}")
            self.reranker = None   #降级策略


    def _load_knowledge_base(self, index_path: str, chunks_path: str):
        """加载知识库。"""
        try:
            if not os.path.exists(index_path) or not os.path.exists(chunks_path):#检查路径是否存在
                logger.error(f"知识库文件缺失: {index_path} 或 {chunks_path} 不存在。")
                return None, None
            index = faiss.read_index(index_path)
            with open(chunks_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                chunks = data.get('chunks', [])#获取json文件中的chunks键值
            logger.info(f"知识库加载成功！共 {len(chunks)} 个文本块。")
            return index, chunks
        except Exception as e:
            logger.error(f"加载知识库时出错: {e}")
            return None, None


    def _build_bm25_index(self, chunks: list[str]):
        """构建BM25索引"""
        if not chunks: return None
        logger.info("正在构建 BM25 关键词索引...")
        try:
            tokenized_chunks = [jieba.lcut(chunk) for chunk in chunks]#用jieba库的lcut方法切词
            bm25 = BM25Okapi(tokenized_chunks)#用切好词的列表创建BM25索引
            logger.info("BM25索引构建完成。")
            return bm25
        except Exception as e:
            logger.error(f"构建 BM25 索引时出错: {e}")
            return None


    #公开方法
    def search(self, query: str, k: int = 5) -> list[dict]:#一般场景：推荐 k=5 或 k=10；精确搜索场景：可以设置为 k=3-5；探索性搜索场景：可以设置为 k=10-15
        """
        执行三阶段搜索：召回（混合检索） -> 融合 -> 重排序。
        """
        if not self.faiss_index or not self.bm25_index:
            logger.error("搜索引擎未正确初始化，无法执行搜索。")
            return []

        recall_k = k * 10#召回数量

        query_vector = get_embedding(query)
        if query_vector is None: return []

        try:
            _, faiss_indices = self.faiss_index.search(np.array([query_vector]).astype('float32'), recall_k)#调用FAISS索引的search方法，_忽略返回的距离
            faiss_indices = faiss_indices[0]#获得索引列表
        except Exception as e:
            logger.error(f"FAISS 搜索出错: {e}");
            faiss_indices = []

        try:
            tokenized_query = jieba.lcut(query)
            bm25_scores = self.bm25_index.get_scores(tokenized_query)#返回分数列表
            bm25_results = sorted([(i, score) for i, score in enumerate(bm25_scores) if score > 0],
                                  key=lambda x: x[1],reverse=True)[:recall_k]#取出分数最高的结果
            bm25_indices = [i for i, score in bm25_results]#取出索引
        except Exception as e:
            logger.error(f"BM25 搜索出错: {e}")
            bm25_indices = []

        all_indices = set(faiss_indices) | set(bm25_indices)#将结果融合
        if not all_indices:
            return []

        recalled_chunks = [self.chunks_with_metadata[i] for i in all_indices]#取出对应的知识块

        if not self.reranker:
            logger.warning("重排序模型未加载，将跳过重排序步骤，直接返回融合结果。")
            return recalled_chunks[:k]#保证返回前k个结果

        logger.info(f"对召回结果进行重排序...")

        pairs = [(query, chunk['text']) for chunk in recalled_chunks]#形成模型需要的元组列表

        scores = self.reranker.predict(pairs)

        scored_chunks = list(zip(scores, recalled_chunks))#生成（分数，知识块）元组

        sorted_chunks = sorted(scored_chunks, key=lambda x: x[0], reverse=True)#排序

        final_chunks = [chunk for score, chunk in sorted_chunks[:k]]#取出元组

        return final_chunks