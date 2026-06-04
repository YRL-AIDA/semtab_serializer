import torch
import faiss
import numpy as np
from transformers import AutoModel, AutoTokenizer
from typing import List, Dict, Tuple, Optional
import re
import pandas as pd

class Qwen3EmbeddingMatcher:
    """
    Поиск определений по косинусному сходству с использованием:
    - Qwen3 Embedding для получения векторов
    - FAISS для хранения и быстрого поиска
    """
    
    def __init__(
        self, 
        definitions: List[str] = None,
        index_name: str = None,
        cossim_model_name: str = "Qwen/Qwen3-Embedding-0.6B",  # Можно также 4B или 8B
        device: Optional[str] = None,
        use_faiss_ip: bool = True,# True = косинусное сходство (Inner Product на нормализованных векторах)
        **kwargs
    ):
        """
        Args:
            definitions: список определений
            cossim_model_name: название модели Qwen3 Embedding
            device: 'cuda' или 'cpu' (auto-detect если None)
            use_faiss_ip: использовать IndexFlatIP (косинусное сходство) или IndexFlatL2
            
        """
        
        self.definitions = definitions
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
            
        print(f"Загрузка модели {cossim_model_name} на {self.device}...")
        self.tokenizer = AutoTokenizer.from_pretrained(cossim_model_name, trust_remote_code=True)
        self.model = AutoModel.from_pretrained(
            cossim_model_name, 
            trust_remote_code=True
        ).to(self.device)
        self.model.eval()
        
        # Получаем размерность эмбеддингов
        self.embedding_dim = self.model.config.hidden_size
        print(f"Размерность эмбеддингов: {self.embedding_dim}")
        if index_name != None:
            self.load_index(index_name)
        elif self.definitions != None:
            
            # Создаем FAISS индекс
            if use_faiss_ip:
                # IndexFlatIP = Inner Product = косинусное сходство для нормализованных векторов
                self.index = faiss.IndexFlatIP(self.embedding_dim)
            else:
                # IndexFlatL2 = Евклидово расстояние
                self.index = faiss.IndexFlatL2(self.embedding_dim)
            
            # Индекс для быстрого доступа к оригинальным определениям
            self.index_to_def = {i: def_text for i, def_text in enumerate(definitions)}
            
            # Создаем эмбеддинги для всех определений
            self._build_index()
        else:
            raise ValueError('definitions or index files not found')
    
    def _preprocess_text(self, text: str) -> str:
        """Предобработка текста (опционально)"""
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    @torch.no_grad()
    def encode(self, texts: List[str]) -> np.ndarray:
        """
        Получение эмбеддингов через Qwen3 Embedding
        
        Args:
            texts: список текстов
            
        Returns:
            numpy array с эмбеддингами
        """
        # Токенизация
        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,  # Можно увеличить до 32768
            return_tensors="pt"
        ).to(self.device)
        
        # Получение эмбеддингов
        outputs = self.model(**inputs)
        
        # Qwen3 Embedding использует последний скрытый состояние для [EOS] токена
        # как семантическое представление [citation:1]
        embeddings = outputs.last_hidden_state[:, -1, :].cpu().numpy()
        
        # Нормализация для косинусного сходства
        faiss.normalize_L2(embeddings)
        
        return embeddings
    
    def _build_index(self):
        """Создание FAISS индекса из определений"""
        print("Создание эмбеддингов для определений...")
        
        # Обрабатываем определения батчами для экономии памяти
        batch_size = 1
        all_embeddings = []
        
        for i in range(0, len(self.definitions), batch_size):
            batch_defs = self.definitions[i:i+batch_size]
            batch_embeddings = self.encode(batch_defs)
            all_embeddings.append(batch_embeddings)
            print(f"Обработано {min(i+batch_size, len(self.definitions))}/{len(self.definitions)}")
        
        # Объединяем все эмбеддинги
        embeddings = np.vstack(all_embeddings)
        
        # Добавляем в FAISS индекс
        self.index.add(embeddings.astype(np.float32))
        print(f"Индекс создан. Всего векторов: {self.index.ntotal}")
    
    def find_similar(
        self, 
        query: str, 
        top_k: int = 5, 
        threshold: float = 0.0
    ) -> List[Dict]:
        """
        Поиск наиболее похожих определений
        
        Args:
            query: поисковый запрос (новый термин)
            top_k: количество результатов
            threshold: минимальный порог сходства
            
        Returns:
            список словарей с результатами
        """
        # Получаем эмбеддинг запроса
        query_embedding = self.encode([query])
        
        # Поиск в FAISS
        similarities, indices = self.index.search(
            query_embedding.astype(np.float32), 
            top_k
        )
        
        # Форматирование результатов
        results = []
        for sim, idx in zip(similarities[0], indices[0]):
            if idx != -1 and sim >= threshold:
                results.append({
                    'index': int(idx),
                    'definition': self.index_to_def[idx],
                    'similarity': float(sim)
                })
        
        return results
    
    def save_index(self, path: str):
        """Сохранение FAISS индекса"""
        faiss.write_index(self.index, f"{path}.faiss")
        # Сохраняем также определения
        np.save(f"{path}_defs.npy", self.definitions)
        print(f"Индекс сохранён в {path}.faiss")
    
    def load_index(self, path: str):
        """Загрузка FAISS индекса"""
        self.index = faiss.read_index(f"{path}.faiss")
        self.definitions = np.load(f"{path}_defs.npy", allow_pickle=True).tolist()
        self.index_to_def = {i: def_text for i, def_text in enumerate(self.definitions)}
        print(f"Индекс загружен. Всего векторов: {self.index.ntotal}")
        
    def annotate_columns(self, df: pd.DataFrame, top_k: int= 1, threshold: float = 0.5,add_data = None)-> List[List[Tuple[str,float]]]:
            
            return [[(res['definition'],round(res['similarity'],4)) 
                    for res in self.find_similar(col_name,top_k=top_k,threshold=threshold)]
                   for col_name in df.columns]