# ingest.py
import os
import faiss
import json
import numpy as np
from openai import OpenAI
import time
from unstructured.partition.auto import partition
from unstructured.documents.elements import Title, Table
import pandas as pd
import io
import config

# 初始化客户端
client = OpenAI(
    api_key=config.API_KEY,
    base_url=config.BASE_URL,
)

# 获取日志记录
logger = config.logger


def process_excel_file(file_path):
    """处理excel文档"""
    logger.info(f"--- 正在使用 pandas 解析 Excel 文件: {file_path} ---")
    try:
        all_sheets_df = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        logger.error(f"读取 Excel 文件时出错: {e}")
        return []

    final_chunks = []
    file_name = os.path.basename(file_path)

    for sheet_name, df in all_sheets_df.items():
        if df.empty: continue
        logger.info(f"  > 正在处理工作表: '{sheet_name}'")
        markdown_table = df.to_markdown(index=False)
        chunk_text = f"在工作表'{sheet_name}'中，找到了以下表格：\n\n{markdown_table}\n\n"
        metadata = {"source": file_name, "sheet_name": sheet_name, "type": "table"}
        final_chunks.append({"text": chunk_text, "metadata": metadata})

    logger.info(f"--- Excel 文件解析完成，共生成 {len(final_chunks)} 个知识块 ---")
    return final_chunks


def process_document_file(file_path, chunk_size=800, chunk_overlap=200):
    """
        处理文本文档
    """
    logger.info(f"正在使用 unstructured 进行解析: {file_path}")
    try:
        elements = partition(file_path, strategy="hi_res")
    except Exception as e:
        logger.error(f"解析文档时出错: {e}")
        return []

    logger.info("正在进行元素分组")
    semantic_blocks = []
    current_text_block = ""
    current_title = "无章节标题"

    for i, el in enumerate(elements):
        if isinstance(el, Table):
            if current_text_block.strip():
                semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})
            table_context = ""
            if i > 0 and not isinstance(elements[i - 1], Title):
                table_context = elements[i - 1].text
            try:
                df = pd.read_html(io.StringIO(el.metadata.text_as_html))[0]
                table_md = df.to_markdown(index=False)
                full_table_content = f"{table_context}\n\n以下是一个表格：\n\n{table_md}\n\n"
                semantic_blocks.append({"type": "table", "content": full_table_content, "title": current_title})
                logger.info("  > 发现并合并了一个带有上下文的表格。")
            except Exception as e:
                logger.warning(f"解析表格失败，将其作为普通文本处理: {e}")
                current_text_block += el.text + "\n\n"
            current_text_block = ""

        elif isinstance(el, Title):
            if current_text_block.strip():
                semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})
            current_title = el.text.strip()
            current_text_block = ""

        else:
            current_text_block += el.text + "\n\n"

    if current_text_block.strip():
        semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})

    logger.info(f"  > 元素分组完成，共得到 {len(semantic_blocks)} 个语义块。")

    final_chunks = []
    file_name = os.path.basename(file_path)
    doc_title = os.path.splitext(file_name)[0]

    for block in semantic_blocks:
        block_text = block["content"]
        block_type = block["type"]
        block_title = block["title"]

        sub_chunks = [block_text[i:i + chunk_size] for i in range(0, len(block_text), chunk_size - chunk_overlap)]

        for sub_chunk in sub_chunks:
            if sub_chunk.strip():
                metadata = {
                    "source": file_name,
                    "doc_title": doc_title,
                    "chapter_title": block_title,
                    "type": block_type
                }
                final_chunks.append({"text": sub_chunk, "metadata": metadata})

    logger.info(f"--- 文件解析和分块完成，共生成 {len(final_chunks)} 个最终知识块 ---")
    return final_chunks


def get_embedding(text_chunk):
    """获取单个文本块的向量"""
    try:
        response = client.embeddings.create(model="text-embedding-v4", input=text_chunk)
        return np.array(response.data[0].embedding)
    except Exception as e:
        logger.error(f"获取 embedding 时出错: {e} - 文本块: '{text_chunk[:30]}...'")
        return None


def get_all_embeddings(chunks):
    """获取所有文本块的向量"""
    logger.info("开始批量向量化所有文本块...")
    valid_chunks = []
    all_vectors = []
    start_time = time.time()
    for i, chunk_dict in enumerate(chunks):
        if (i + 1) % 10 == 0:
            logger.info(f"  正在处理第 {i + 1}/{len(chunks)} 个文本块...")
        text_to_embed = chunk_dict.get("text", "")
        if not text_to_embed or not text_to_embed.strip():
            continue
        vector = get_embedding(text_to_embed)
        if vector is not None:
            valid_chunks.append(chunk_dict)
            all_vectors.append(vector)
    end_time = time.time()
    logger.info(f"批量向量化完成，共{len(all_vectors)}个向量，耗时 {end_time - start_time:.2f} 秒。")
    return valid_chunks, all_vectors


def build_and_save_artifacts(vectors, chunks, index_path, chunks_path):
    """构建并保存FAISS索引和文本块"""
    if not vectors or not chunks:
        logger.warning("向量或文本块为空，不执行建库与保存操作")
        return

    logger.info("--- 正在构建并保存 FAISS 索引和文本块 ---")
    d = vectors[0].shape[0]
    index = faiss.IndexFlatL2(d)
    logger.info(f"正在向索引中添加 {len(vectors)} 个向量...")
    index.add(np.array(vectors).astype('float32'))
    logger.info("所有向量已成功添加到索引！")
    logger.info(f"正在将索引保存到: {index_path}")
    faiss.write_index(index, index_path)
    logger.info(f"正在将文本块保存到: {chunks_path}")
    with open(chunks_path, 'w', encoding='utf-8') as f:
        json.dump({"chunks": chunks}, f, ensure_ascii=False, indent=4)
    logger.info("所有成果已成功保存！")


def run_ingestion_pipeline():
    """
    遍历源文档目录，处理所有支持的文件，并构建统一的知识库。
    """
    source_dir = config.SOURCE_DOCS_DIR
    if not os.path.isdir(source_dir):
        logger.error(f"错误：源文档目录 '{source_dir}' 不存在。请创建该目录并放入文件。")
        return

    all_chunks = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_extension = os.path.splitext(file)[1].lower()

            logger.info(f"--- 开始处理文件: {file_path} ---")
            chunks = []
            if file_extension in ['.xlsx', '.xls']:
                chunks = process_excel_file(file_path)
            elif file_extension == '.docx':
                chunks = process_document_file(file_path)
            else:
                logger.warning(f"跳过不支持的文件类型: {file}")
                continue

            all_chunks.extend(chunks)
            logger.info(f"文件 {file} 处理完成，获得 {len(chunks)} 个知识块。")

    if not all_chunks:
        logger.warning("未能从任何文件中提取出知识块，处理终止。")
        return

    logger.info(f"所有文件处理完毕，共计 {len(all_chunks)} 个知识块。开始进行向量化...")
    valid_chunks, vectors = get_all_embeddings(all_chunks)
    os.makedirs(config.GENERATED_DATA_DIR, exist_ok=True)
    build_and_save_artifacts(vectors, valid_chunks, config.INDEX_FILE_PATH, config.CHUNKS_FILE_PATH)
    logger.info(f"\n--- 统一知识库构建完成！---")
    logger.info(f"索引保存至: {config.INDEX_FILE_PATH}")
    logger.info(f"数据保存至: {config.CHUNKS_FILE_PATH}")


if __name__ == "__main__":
    run_ingestion_pipeline()