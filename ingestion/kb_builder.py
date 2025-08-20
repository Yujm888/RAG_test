# ingestion/kb_builder.py
import faiss
import json
import numpy as np
import config

logger = config.logger

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
