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
        all_sheets_df = pd.read_excel(file_path, sheet_name=None)       #读取工作簿中的工作表(sheet_name=None表示读取所有工作表)
    except Exception as e:
        logger.error(f"读取 Excel 文件时出错: {e}")
        return []

    final_chunks = []
    file_name = os.path.basename(file_path)     #获取文件名

    for sheet_name, df in all_sheets_df.items():
        if df.empty: continue
        logger.info(f"  > 正在处理工作表: '{sheet_name}'")
        markdown_table = df.to_markdown(index=False)        #index=False参数表示不包含行索引，只输出数据内容
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
        elements = partition(file_path, strategy="hi_res")  #自动识别文件中的文本,strategy为精度参数，返回一个list（text;title;table;listitem）
    except Exception as e:
        logger.error(f"解析文档时出错: {e}")
        return []

    logger.info("正在进行元素分组")
    semantic_blocks = []        #存储文本块的列表
    current_text_block = ""     #字符串，用于临时存储当前文本块
    current_title = "无章节标题"    #默认章节标题

    for i, el in enumerate(elements):       #enumerate:同时返回索引i和值el
        if isinstance(el, Table):       #isinstance:判断对象是否是某个类的实例
            if current_text_block.strip():      #str.strip()：移除字符串首尾的空格
                semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})     #检查文本块是否为空，若不为空就添加到文本列表中
            table_context = ""
            if i > 0 and not isinstance(elements[i - 1], Title):
                table_context = elements[i - 1].text    #获取表格的标题，若无则为上个文本块
            try:
                df = pd.read_html(io.StringIO(el.metadata.text_as_html))[0]     #elements中的表格一般为html格式，io.StringIO配合pandas.read_html使用，最终返回一个DataFrame列表
                table_md = df.to_markdown(index=False)      #将得到的DataFrame格式转换为markdown格式
                full_table_content = f"{table_context}\n\n以下是一个表格：\n\n{table_md}\n\n"
                semantic_blocks.append({"type": "table", "content": full_table_content, "title": current_title})
                logger.info("处理了一个的表格。")
            except Exception as e:
                logger.warning(f"解析表格失败，将其作为普通文本处理: {e}")
                current_text_block += el.text + "\n\n"
            current_text_block = ""     #重置当前文本块

        elif isinstance(el, Title):     #处理标题
            if current_text_block.strip():
                semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})
            current_title = el.text.strip()
            current_text_block = ""

        else:
            current_text_block += el.text + "\n\n"

    if current_text_block.strip():
        semantic_blocks.append({"type": "text", "content": current_text_block.strip(), "title": current_title})

    logger.info(f"文本分组完成，共得到 {len(semantic_blocks)} 个语义块。")

    final_chunks = []
    file_name = os.path.basename(file_path)     #获取文件名
    doc_title = os.path.splitext(file_name)[0]      #os.path.splitext：将文件名与扩展名分开，返回一个元组

    for block in semantic_blocks:
        block_text = block["content"]
        block_type = block["type"]
        block_title = block["title"]

        sub_chunks = [block_text[i:i + chunk_size] for i in range(0, len(block_text), chunk_size - chunk_overlap)]      #[表达式 for 变量 in 可迭代对象]

        for sub_chunk in sub_chunks:
            if sub_chunk.strip():
                metadata = {
                    "source": file_name,
                    "doc_title": doc_title,
                    "chapter_title": block_title,
                    "type": block_type
                }
                final_chunks.append({"text": sub_chunk, "metadata": metadata})      #将最终的文本块和元数据封装成字典

    logger.info(f"--- 文件解析和分块完成，共生成 {len(final_chunks)} 个最终知识块 ---")
    return final_chunks


def get_embedding(text_chunk):
    """获取单个文本块的向量"""
    try:
        response = client.embeddings.create(model="text-embedding-v4", input=text_chunk)
        return np.array(response.data[0].embedding)     #response(返回的为CreateEmbeddingResponse对象).data[0]（第一个文本的元素，由于输入只有一个，这也是唯一一个）.embedding（元素列表中的向量）
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


def build_and_save_artifacts(vectors, chunks, index_path, chunks_path):
    """构建并保存FAISS索引和文本块文件"""
    if not vectors or not chunks:
        logger.warning("向量或文本块为空，不执行建库与保存操作")
        return      #值为空提前终止

    logger.info("正在构建并保存 FAISS 索引和文本块")
    d = vectors[0].shape[0]      #获取向量的维度
    index = faiss.IndexFlatL2(d)        #初始化FAISS索引(维度为d)；IndexFlatL2：特定类型索引，可更换为其他索引类型
    logger.info(f"正在向索引中添加 {len(vectors)} 个向量...")
    index.add(np.array(vectors).astype('float32'))      #.astype:强制转换类型；np.array():将列表转换为二维数组
    logger.info("所有向量已成功添加到索引！")
    logger.info(f"正在将索引保存到: {index_path}")
    faiss.write_index(index, index_path)        #将index写入index_path
    logger.info(f"正在将文本块保存到: {chunks_path}")
    with open(chunks_path, 'w', encoding='utf-8') as f:     #with open() as f:创建对象f并在执行完毕后关闭;'w':以写入模式打开
        json.dump({"chunks": chunks}, f, ensure_ascii=False, indent=4)      #写入设置，具体查询json帮助文档
    logger.info("所有成果已成功保存！")


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