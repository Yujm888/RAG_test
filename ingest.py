# ingest.py
import os
import config
import logging
for handler in config.logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setLevel(logging.INFO)
from ingestion.parsers import process_excel_file, process_document_file
from ingestion.vectorizer import get_all_embeddings
from ingestion.kb_builder import build_and_save_artifacts

logger = config.logger

def run_ingestion_pipeline():
    """
    遍历源文档目录，处理所有支持的文件，并构建统一的知识库。
    """
    source_dir = config.SOURCE_DOCS_DIR     #从config中读取文档目录的路径配置
    if not os.path.isdir(source_dir):       #启动前检查源文档目录
        logger.error(f"错误：源文档目录 '{source_dir}' 不存在。请创建该目录并放入文件。")
        return

    all_chunks = []
    for root, dirs, files in os.walk(source_dir):       #遍历目录树
        for file in files:
            file_path = os.path.join(root, file)        #拼接文件路径
            file_extension = os.path.splitext(file)[1].lower()      #获取并格式化文件扩展名(将扩展名小写)

            logger.info(f"--- 开始处理文件: {file_path} ---")
            chunks = []
            #判断文件类型，可添加修改文件类型
            if file_extension in ['.xlsx', '.xls']:
                chunks = process_excel_file(file_path)
            elif file_extension == '.docx':
                chunks = process_document_file(file_path)
            else:
                logger.warning(f"跳过不支持的文件类型: {file}")
                continue

            all_chunks.extend(chunks)       #list.extend()：将一个列表的元素一个一个添加到另一个列表的末尾
            logger.info(f"文件 {file} 处理完成，获得 {len(chunks)} 个知识块。")

    if not all_chunks:
        logger.warning("未能从任何文件中提取出知识块，处理终止。")
        return

    logger.info(f"所有文件处理完毕，共计 {len(all_chunks)} 个知识块。开始进行向量化...")
    valid_chunks, vectors = get_all_embeddings(all_chunks)
    os.makedirs(config.GENERATED_DATA_DIR, exist_ok=True)       #创建用于存放生成文件的目录
    build_and_save_artifacts(vectors, valid_chunks, config.INDEX_FILE_PATH, config.CHUNKS_FILE_PATH)        #保存生成的文件
    logger.info(f"\n--- 知识库构建完成！---")
    logger.info(f"索引保存至: {config.INDEX_FILE_PATH}")
    logger.info(f"数据保存至: {config.CHUNKS_FILE_PATH}")


if __name__ == "__main__":      #执行模块
    run_ingestion_pipeline()
