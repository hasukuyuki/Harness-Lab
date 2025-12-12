import pickle
import os
import hashlib
from typing import List, Dict, Any, Tuple

class VectorDB:
    """向量数据库模块 - 简化版本，基于文本相似度的知识检索"""
    
    def __init__(self, db_path: str = "data/vector_db"):
        self.db_path = db_path
        self.metadata_path = os.path.join(db_path, "metadata.pkl")
        
        # 确保目录存在
        os.makedirs(db_path, exist_ok=True)
        
        # 简化版本，使用文本存储
        self.metadata = []
        self._load_or_create_index()
    
    def _load_or_create_index(self):
        """加载或创建索引"""
        if os.path.exists(self.metadata_path):
            # 加载现有索引
            with open(self.metadata_path, 'rb') as f:
                self.metadata = pickle.load(f)
        else:
            # 创建新索引
            self.metadata = []
            self._save_index()
    
    def _save_index(self):
        """保存索引和元数据"""
        with open(self.metadata_path, 'wb') as f:
            pickle.dump(self.metadata, f)
    
    async def add_document(self, text: str, doc_id: str, metadata: Dict[str, Any] = None):
        """添加文档到向量数据库"""
        # 添加元数据
        doc_metadata = {
            "doc_id": doc_id,
            "text": text,
            "metadata": metadata or {},
            "vector_id": len(self.metadata)
        }
        self.metadata.append(doc_metadata)
        
        # 保存索引
        self._save_index()
    
    async def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """搜索相关文档 - 简化版本，基于关键词匹配"""
        if not self.metadata:
            return []
        
        query_lower = query.lower()
        query_words = set(query_lower.split())
        
        # 计算相似度分数
        scored_docs = []
        for doc_metadata in self.metadata:
            text_lower = doc_metadata["text"].lower()
            text_words = set(text_lower.split())
            
            # 简单的词汇重叠相似度
            common_words = query_words.intersection(text_words)
            if common_words:
                score = len(common_words) / len(query_words.union(text_words))
                scored_docs.append((score, doc_metadata))
        
        # 按分数排序
        scored_docs.sort(key=lambda x: x[0], reverse=True)
        
        # 构建结果
        results = []
        for score, doc_metadata in scored_docs[:top_k]:
            result = {
                "doc_id": doc_metadata["doc_id"],
                "text": doc_metadata["text"],
                "score": float(score),
                "metadata": doc_metadata["metadata"]
            }
            results.append(result)
        
        return results
    
    async def add_knowledge_base(self, knowledge_files: List[str]):
        """批量添加知识库文件"""
        for file_path in knowledge_files:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 将长文档分块
                chunks = self._split_text(content)
                
                for i, chunk in enumerate(chunks):
                    doc_id = f"{os.path.basename(file_path)}_chunk_{i}"
                    await self.add_document(
                        text=chunk,
                        doc_id=doc_id,
                        metadata={"source_file": file_path, "chunk_index": i}
                    )
    
    def _split_text(self, text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
        """将长文本分块"""
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            # 尝试在句号处分割
            if end < len(text):
                last_period = text.rfind('。', start, end)
                if last_period != -1 and last_period > start + chunk_size // 2:
                    end = last_period + 1
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            start = end - overlap
        
        return chunks
    
    def get_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        return {
            "total_documents": len(self.metadata),
            "index_size": len(self.metadata)
        }