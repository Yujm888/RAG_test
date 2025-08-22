# 这是一个专门用于手动刷新 Schema 缓存的工具脚本。
from core.schema_fetcher import BaseSchemaFetcher

print("--- Schema 缓存刷新工具 ---")

# 调用我们之前写好的静态方法来删除缓存文件
BaseSchemaFetcher.clear_cache()

print("\n操作完成。")
print("如果之前存在旧的缓存文件，它现在已被删除。")
print("下一次应用启动时，将会从数据库重新提取最新的 Schema 并生成新缓存。")