'''
title: RagFlow Pipeline
author: luyilong2015 & Gemini
date: 2025-10-31
version: 1.4
license: MIT
description: A pipeline for retrieving relevant information from a knowledge base using the RagFlow's Agent Interface. This version uses a synchronous pipe function with an async runner for robust stream handling.
requirements: httpx, requests
"""

from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel
import requests
import httpx
import json
import logging
import asyncio

log = logging.getLogger(__name__)

class Pipeline:
    class Valves(BaseModel):
        API_KEY: str
        AGENT_ID: str
        HOST: str
        PORT: str

    def __init__(self):
        self.session_id = None
        self.debug = True
        self.sessionKV = {}
        self.valves = self.Valves(
            **{
                "API_KEY": "",
                "AGENT_ID": "",
                "HOST": "",
                "PORT": ""
            }
        )

    async def on_startup(self):
        log.info("RagFlow Pipeline starting up.")

    async def on_shutdown(self):
        log.info("RagFlow Pipeline shutting down.")

    # inlet 必须是异步的
    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        if not self.debug: return body
        
        chat_id = body.get('metadata', {}).get('chat_id')
        if not chat_id:
            log.warning("Could not find chat_id in inlet body.")
            return body

        log.info(f"Inlet: Handling chat_id: {chat_id}")

        if chat_id in self.sessionKV:
            self.session_id = self.sessionKV[chat_id]
            log.info(f"Found cached RAGFlow session_id: {self.session_id}")
        else:
            log.info("No cached session_id. Creating a new RAGFlow session...")
            session_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/agents/{self.valves.AGENT_ID}/sessions"
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.valves.API_KEY}'
            }
            try:
                # 在异步函数 inlet 中，我们可以安全地使用 httpx
                async with httpx.AsyncClient(verify=False) as client:
                    response = await client.post(session_url, headers=headers, json={}, timeout=30)
                    response.raise_for_status()
                    json_res = response.json()
                
                self.session_id = json_res.get('data', {}).get('id')
                if self.session_id:
                    self.sessionKV[chat_id] = self.session_id
                    log.info(f"Created new RAGFlow session_id: {self.session_id}")
                else:
                    log.error(f"Failed to extract session_id from RAGFlow response: {json_res}")
            except Exception as e:
                log.error(f"Failed to create RAGFlow session: {repr(e)}")
                raise e
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        return body

    # pipe 必须是同步的
    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        
        # 创建一个内部的异步函数来处理流式请求
        async def stream_ragflow_response():
            if not self.session_id:
                yield "Error: RAGFlow session not initialized. Please start a new chat."
                return

            question_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/agents/{self.valves.AGENT_ID}/completions"
            headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.valves.API_KEY}'}
            question_data = {'question': user_message, 'stream': True, 'session_id': self.session_id, 'lang': 'Chinese'}

            try:
                async with httpx.AsyncClient(timeout=300, verify=False) as client:
                    async with client.stream("POST", question_url, headers=headers, json=question_data) as response:
                        response.raise_for_status()
                        full_answer = ""
                        async for line in response.aiter_lines():
                            if line.startswith("data:"):
                                json_data_str = line[5:].strip()
                                if not json_data_str or json_data_str == '[DONE]':
                                    continue
                                
                                try:
                                    json_data = json.loads(json_data_str)
                                    if 'data' in json_data and isinstance(json_data['data'], dict):
                                        if 'answer' in json_data['data'] and '* is running...' not in json_data['data']['answer']:
                                            new_chunk = json_data['data']['answer']
                                            if len(new_chunk) > len(full_answer):
                                                yield new_chunk[len(full_answer):]
                                                full_answer = new_chunk
                                        
                                        if json_data['data'].get('reference'):
                                            referenceStr = "\n\n### References\n"
                                            filesList = set()
                                            for chunk in json_data['data']['reference'].get('chunks', []):
                                                doc_id = chunk.get('document_id')
                                                if doc_id and doc_id not in filesList:
                                                    filename = chunk.get('document_name', 'Unknown')
                                                    ext = filename.split('.')[-1].strip().lower() if '.' in filename else ''
                                                    ref_url = f"{self.valves.HOST}:{self.valves.PORT}/document/{doc_id}?ext={ext}&prefix=document"
                                                    referenceStr += f"\n- [{filename}]({ref_url})"
                                                    filesList.add(doc_id)
                                            yield referenceStr
                                except json.JSONDecodeError:
                                    log.warning(f"Failed to parse JSON from stream: {line}")
            except Exception as e:
                log.error(f"An unexpected error occurred in pipe: {repr(e)}")
                yield f"An unexpected error occurred: {repr(e)}"

        # 在同步的 pipe 函数中，运行并返回异步生成器的结果
        try:
            # 获取或创建一个事件循环
            loop = asyncio.get_running_loop()
        except RuntimeError:  # 'get_running_loop' fails in non-running thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # 创建一个生成器，用于在同步函数中迭代异步生成器的结果
        async_gen = stream_ragflow_response()
        try:
            while True:
                # 在事件循环中运行异步生成器的下一步
                yield loop.run_until_complete(async_gen.__anext__())
        except StopAsyncIteration:
            pass
