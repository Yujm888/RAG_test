# ingestion/parsers.py
import pandas as pd
import io
from unstructured.partition.auto import partition
from unstructured.documents.elements import Title, Table
import os
import config

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
