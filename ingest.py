# ingest.py (面向对象版本)
import os
import config
import logging
from ingestion.parsers import process_excel_file, process_document_file
from ingestion.vectorizer import get_all_embeddings
from ingestion.kb_builder import build_and_save_artifacts

logger = config.logger


class IngestionPipeline:
    """
    封装了从源文件处理到构建知识库（向量索引和数据块）的完整流程。
    """

    def __init__(self, source_dir, generated_dir, index_file_name, chunks_file_name):
        """
        初始化构建流程的配置。
        """
        self.source_dir = source_dir
        self.generated_dir = generated_dir
        self.index_path = os.path.join(generated_dir, index_file_name)
        self.chunks_path = os.path.join(generated_dir, chunks_file_name)

        # 定义支持的文件解析器映射
        self.supported_parsers = {
            '.xlsx': process_excel_file,
            '.xls': process_excel_file,
            '.docx': process_document_file,
        }


    def _process_single_file(self, file_path):
        """处理单个文件，根据其扩展名选择合适的解析器。"""
        file_extension = os.path.splitext(file_path)[1].lower()
        parser_func = self.supported_parsers.get(file_extension)

        if parser_func:
            return parser_func(file_path)
        else:
            logger.warning(f"跳过不支持的文件类型: {os.path.basename(file_path)}")
            return []


    def run(self):
        """
        遍历源文档目录，处理所有支持的文件，并构建统一的知识库。
        """
        source_dir = self.source_dir  # 从 self 中读取配置
        if not os.path.isdir(source_dir):  # 启动前检查源文档目录
            logger.error(f"错误：源文档目录 '{source_dir}' 不存在。请创建该目录并放入文件。")
            return

        all_chunks = []
        for root, dirs, files in os.walk(source_dir):  # 遍历目录树
            for file in files:
                file_path = os.path.join(root, file)  # 拼接文件路径

                logger.info(f"--- 开始处理文件: {file_path} ---")
                chunks = self._process_single_file(file_path)

                if chunks:
                    all_chunks.extend(chunks)  # list.extend()：将一个列表的元素一个一个添加到另一个列表的末尾
                    logger.info(f"文件 {file} 处理完成，获得 {len(chunks)} 个知识块。")

        if not all_chunks:
            logger.warning("未能从任何文件中提取出知识块，处理终止。")
            return

        logger.info(f"所有文件处理完毕，共计 {len(all_chunks)} 个知识块。开始进行向量化...")
        valid_chunks, vectors = get_all_embeddings(all_chunks)

        os.makedirs(self.generated_dir, exist_ok=True)  # 创建用于存放生成文件的目录

        # 使用 self 中的路径属性保存文件
        build_and_save_artifacts(vectors, valid_chunks, self.index_path, self.chunks_path)

        logger.info(f"\n--- 知识库构建完成！---")
        logger.info(f"索引保存至: {self.index_path}")
        logger.info(f"数据保存至: {self.chunks_path}")


# 主执行入口
if __name__ == "__main__":  # 执行模块
    # 配置日志，确保 INFO 级别信息能输出到控制台
    for handler in config.logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(logging.INFO)

    # 1. 使用 config 文件中的配置，创建一个 IngestionPipeline 实例
    pipeline = IngestionPipeline(
        source_dir=config.SOURCE_DOCS_DIR,
        generated_dir=config.GENERATED_DATA_DIR,
        index_file_name=config.KB_INDEX_FILE,
        chunks_file_name=config.KB_CHUNKS_FILE
    )

    # 2. 调用实例的 run 方法，启动知识库构建流程
    pipeline.run()