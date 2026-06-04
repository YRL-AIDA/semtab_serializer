import re
import json
import logging
import asyncio
import httpx
import pandas as pd
from typing import List, Dict, Optional
from openai import AsyncOpenAI

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def remove_think_tags(text: str) -> str:
    """Удаляет тег <think>...</think> из ответов моделей reasoning."""
    try:
        match = re.search(r"<\/think>", text)
        if match:
            return text[match.end():].strip()
    except Exception as e:
        print(f"Error removing <think> tags: {e}")
    return text.strip()

class LLMClient:
    def __init__(self, base_url: str, tokenizer_url: str, api_key: str = 'EMPTY'):
        # 1. ДОБАВЛЕН ТАЙМАУТ: Если локальная модель зависла, скрипт отвалится через 10 минут, а не зависнет навсегда.
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=600.0)
        self.tokenizer_url = tokenizer_url
        
        # 2. ОГРАНИЧЕНИЕ ПУЛА: Не даем httpx создавать бесконечное число соединений.
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=10)
        self.http_client = httpx.AsyncClient(timeout=10.0, limits=limits)
        
        # 3. СЕМАФОР ТОКЕНИЗАТОРА: Отправляем не более 30 запросов к токенизатору одновременно.
        self.token_semaphore = asyncio.Semaphore(30)

    async def generate(self, messages: List[Dict[str, str]], model: str, **kwargs) -> Optional[str]:
        try:
            response = await self.client.chat.completions.create(
                messages=messages,
                model=model,
                **kwargs
            )
            raw_content = response.choices[0].message.content
            return remove_think_tags(raw_content)
        except Exception as e:
            logger.error(f"Failed to call LLM: {e}")
            return None

    async def count_tokens(self, text: str, model: str) -> int:
        # ЗАЩИТА: Строго ограничиваем конкурентность запросов к токенизатору
        async with self.token_semaphore:
            try:
                response = await self.http_client.post(
                    self.tokenizer_url,
                    json={"model": model, "prompt": text}
                )
                response.raise_for_status()
                return response.json().get('count', len(text) // 4)
            except Exception as e:
                # Убрала логирование ошибки, чтобы не спамить консоль тысячами сообщений при таймауте
                return len(text) // 4 + 1
            
    async def close(self):
        await self.http_client.aclose()
        await self.client.close()

class AsyncErrorSummarizer:
    def __init__(self, client: LLMClient, model: str, max_chunk_tokens: int = 6000, max_len: int = 20000, max_concurrent_tasks: int = 5):
        self.client = client
        self.model = model
        self.max_chunk_tokens = max_chunk_tokens
        self.max_len = max_len 
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

    def _extract_description(self, row) -> Optional[str]:
        try:
            data = json.loads(row['llm_analysis'])
            desc = data.get('error_description', '')
            err_type = row.get('err_type', 'UNKNOWN')
            if not desc: 
                return None
            return f"<{err_type}>: {desc}"
        except Exception:
            return None

    async def _get_chunks(self, descriptions: List[str]) -> List[str]:
        chunks = []
        current_batch = []
        current_tokens = 0
        
        logger.info(f"Counting tokens for {len(descriptions)} items...")
        
        async def fetch_tokens(desc: str) -> int:
            return await self.client.count_tokens(desc, self.model)
        
        # Разбиваем на батчи встроенными средствами asyncio, чтобы не повесить Event Loop
        token_counts = await asyncio.gather(*(fetch_tokens(desc) for desc in descriptions))
        
        logger.info("Token counting completed. Building chunks...")
        
        for desc, tokens in zip(descriptions, token_counts):
            if tokens > self.max_chunk_tokens:
                logger.warning(f"Skipping single item: size ({tokens} tokens) exceeds max_chunk_tokens.")
                continue

            if current_tokens + tokens > self.max_chunk_tokens:
                chunks.append("\n\n".join(current_batch))
                current_batch = [desc]
                current_tokens = tokens
            else:
                current_batch.append(desc)
                current_tokens += tokens
        
        if current_batch:
            chunks.append("\n\n".join(current_batch))
            
        return chunks

    async def summarize_chunk(self, chunk_text: str, chunk_index: int, total_chunks: int) -> Optional[str]:
        prompt = f"""You are a helpful assistant. Summarize the following error descriptions into a concise technical report. 
        Maintain all key details about error patterns.
        
        -Data-
        {chunk_text}
        
        Output:"""
        
        async with self.semaphore:
            logger.info(f"Generating summary for chunk {chunk_index + 1}/{total_chunks}...")
            res = await self.client.generate(
                [{"role": "user", "content": prompt}], 
                self.model, 
                temperature=0
            )
            logger.info(f"Chunk {chunk_index + 1}/{total_chunks} completed.")
            return res

    async def process_group(self, df: pd.DataFrame, group_name: str) -> str:
        logger.info(f"[{group_name}] Extracting data...")
        descriptions = df.apply(self._extract_description, axis=1).dropna().tolist()
        if not descriptions:
            return group_name, "No valid data to summarize."

        chunks = await self._get_chunks(descriptions)
        logger.info(f"[{group_name}] Total chunks to process: {len(chunks)}")
        
        if not chunks:
             return "All items were too large and were skipped."

        tasks = [self.summarize_chunk(c, i, len(chunks)) for i, c in enumerate(chunks)]
        intermediate_summaries = await asyncio.gather(*tasks)
        
        valid_summaries = [s for s in intermediate_summaries if s]
        if not valid_summaries:
            return "Failed to generate intermediate summaries."

        logger.info(f"[{group_name}] Intermediate map step done. Preparing final reduce...")
        
        base_prompt = """You are a technical expert. Based on the intermediate summaries below, perform two tasks:
        1. Identify a comprehensive name for this error group.
        2. Write a single, cohesive summary of all errors in the third person.
        
        Constraints:
        - The summary must NOT exceed 5 sentences.
        - Resolve any contradictions.
        - Address the error group by the name you identified.
        
        -Intermediate Summaries-
        {DATA}
        
        Output format:
        Group Name: [Your generated name]
        Summary: [Your 5-sentence summary]"""

        base_tokens = await self.client.count_tokens(base_prompt, self.model)
        available_tokens = self.max_len - base_tokens - 1000 
        
        final_summaries_to_include = []
        current_combined_tokens = 0
        
        summary_tokens = await asyncio.gather(
            *(self.client.count_tokens(s, self.model) for s in valid_summaries)
        )
        
        for summary, tokens in zip(valid_summaries, summary_tokens):
            if current_combined_tokens + tokens > available_tokens:
                logger.warning(f"[{group_name}] Reached max_len limit. Skipping remaining summaries.")
                break
                
            final_summaries_to_include.append(summary)
            current_combined_tokens += tokens
            
        if not final_summaries_to_include:
             return "Error: Initial summaries are too large to fit in max_len."

        combined_summaries_text = "\n---\n".join(final_summaries_to_include)
        final_prompt = base_prompt.replace("{DATA}", combined_summaries_text)

        logger.info(f"[{group_name}] Sending final prompt to LLM...")
        async with self.semaphore:
            final_result = await self.client.generate(
                [{"role": "user", "content": final_prompt}], 
                self.model, 
                temperature=0.2
            )
            
        logger.info(f"[{group_name}] Final summary completed.")
        return group_name,final_result or "Failed to generate final summary."

async def main():
    client = LLMClient(base_url="http://localhost:9124/v1", tokenizer_url="http://localhost:9124/tokenize")
    
    # ВАЖНО: Если у вас локально запущена тяжелая модель (30B), 
    # параллельная обработка 3 запросов с большим контекстом требует огромного объема VRAM.
    # Если скрипт все еще висит - поставьте max_concurrent_tasks=1
    summarizer = AsyncErrorSummarizer(client, model="Qwen/Qwen3-30B-A3B-Thinking-2507", max_concurrent_tasks=16)
    
    try:
        logger.info("Loading parquet file...")
        df = pd.read_parquet('data4_80ac.parquet')    

        logger.info("Starting parallel group processing...")
        results = await asyncio.gather(
            *[summarizer.process_group(group, name) for name,group in df.groupby('cluster')]
        )
        print(results)
        for i, res in results :
            
            print(f"\n--- RESULT GROUP {i} ---\n{res}")
            
    except FileNotFoundError:
        logger.error("File 'data.parquet' not found.")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
