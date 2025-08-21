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

    def __init__(self, index_path: str, chunks_path: str):
        logger.info("--- 正在初始化搜索引擎 ---")
        self.faiss_index = None
        self.chunks_with_metadata = []
        self.plain_chunks = []
        self.bm25_index = None
        self.reranker = None

        self.faiss_index, self.chunks_with_metadata = self._load_knowledge_base(index_path, chunks_path)

        if self.faiss_index:
            self.plain_chunks = [item['text'] for item in self.chunks_with_metadata]
            self.bm25_index = self._build_bm25_index(self.plain_chunks)
            self._load_reranker_model()

            if self.bm25_index and self.reranker:
                logger.info("--- 搜索引擎初始化成功 (FAISS + BM25 + Re-ranker) ---")
            else:
                logger.warning("--- 搜索引擎初始化不完整 (请检查BM25或Re-ranker) ---")
        else:
            logger.error("搜索引擎初始化失败，因为无法加载知识库。")

    def _load_reranker_model(self):
        """加载重排序模型。"""
        try:
            logger.info("正在加载重排序模型 (bge-reranker-base)...")
            self.reranker = CrossEncoder('BAAI/bge-reranker-base', max_length=512)
            logger.info("重排序模型加载成功。")
        except Exception as e:
            logger.error(f"加载重排序模型失败: {e}")
            self.reranker = None

    def _load_knowledge_base(self, index_path: str, chunks_path: str):
        try:
            if not os.path.exists(index_path) or not os.path.exists(chunks_path):
                logger.error(f"知识库文件缺失: {index_path} 或 {chunks_path} 不存在。")
                return None, None
            index = faiss.read_index(index_path)
            with open(chunks_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                chunks = data.get('chunks', [])
            logger.info(f"知识库加载成功！共 {len(chunks)} 个文本块。")
            return index, chunks
        except Exception as e:
            logger.error(f"加载知识库时出错: {e}")
            return None, None

    def _build_bm25_index(self, chunks: list[str]):
        if not chunks: return None
        logger.info("正在使用 jieba 构建 BM25 关键词索引...")
        try:
            tokenized_chunks = [jieba.lcut(chunk) for chunk in chunks]
            bm25 = BM25Okapi(tokenized_chunks)
            logger.info("BM25 (jieba) 索引构建完成。")
            return bm25
        except Exception as e:
            logger.error(f"构建 BM25 索引时出错: {e}")
            return None

    def search(self, query: str, k: int = 5) -> list[dict]:
        """
        执行三阶段搜索：召回 -> RRF融合 -> 重排序。
        """
        if not self.faiss_index or not self.bm25_index:
            logger.error("搜索引擎未正确初始化，无法执行搜索。")
            return []

        recall_k = k * 10

        query_vector = get_embedding(query)
        if query_vector is None: return []

        try:
            _, faiss_indices = self.faiss_index.search(np.array([query_vector]).astype('float32'), recall_k)
            faiss_indices = faiss_indices[0]
        except Exception as e:
            logger.error(f"FAISS 搜索出错: {e}");
            faiss_indices = []

        try:
            tokenized_query = jieba.lcut(query)
            bm25_scores = self.bm25_index.get_scores(tokenized_query)
            bm25_results = sorted([(i, score) for i, score in enumerate(bm25_scores) if score > 0], key=lambda x: x[1],
                                  reverse=True)[:recall_k]
            bm25_indices = [i for i, score in bm25_results]
        except Exception as e:
            logger.error(f"BM25 搜索出错: {e}");
            bm25_indices = []

        all_indices = set(faiss_indices) | set(bm25_indices)
        if not all_indices:
            return []

        recalled_chunks = [self.chunks_with_metadata[i] for i in all_indices]

        if not self.reranker:
            logger.warning("重排序模型未加载，将跳过重排序步骤，直接返回融合结果。")
            return recalled_chunks[:k]

        logger.info(f"开始对 {len(recalled_chunks)} 个召回结果进行重排序...")

        pairs = [(query, chunk['text']) for chunk in recalled_chunks]

        scores = self.reranker.predict(pairs)

        scored_chunks = list(zip(scores, recalled_chunks))

        sorted_chunks = sorted(scored_chunks, key=lambda x: x[0], reverse=True)

        final_chunks = [chunk for score, chunk in sorted_chunks[:k]]

        return final_chunks