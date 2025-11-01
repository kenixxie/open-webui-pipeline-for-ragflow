
'''
title: RagFlow Pipeline
author: luyilong2015 & Gemini
date: 2025-10-31
version: 1.3
license: MIT
description: A pipeline for retrieving relevant information from a knowledge base using the RagFlow's Agent Interface. This version uses httpx for robust asynchronous stream handling and includes detailed pre-request debugging.
requirements: httpx
"""

from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel
import httpx
import json
import logging
import asyncio
import time

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
                # 使用同步的 requests 来创建 session，避免在 inlet 中引入复杂异步问题
                session_response = httpx.post(session_url, headers=headers, json={}, verify=False)
                session_response.raise_for_status()
                json_res = session_response.json()
                
                self.session_id = json_res.get('data', {}).get('id')
                if self.session_id:
                    self.sessionKV[chat_id] = self.session_id
                    log.info(f"Created new RAGFlow session_id: {self.session_id}")
                else:
                    log.error(f"Failed to extract session_id from RAGFlow response: {json_res}")
            except Exception as e:
                log.error(f"Failed to create RAGFlow session: {repr(e)}")
                # 抛出异常，让上层知道 inlet 失败
                raise e
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        return body

    async def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        
        if not self.session_id:
            yield "Error: RAGFlow session not initialized. Please start a new chat or send a new message to create one."
            return

        question_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/agents/{self.valves.AGENT_ID}/completions"
        question_headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.valves.API_KEY}'
        }
        question_data = {
            'question': user_message,
            'stream': True,
            'session_id': self.session_id,
            'lang': 'Chinese'
        }

        # ================== 终极调试日志 ==================
        log.info("############################################################")
        log.info("### RAGFLOW PIPE DEBUG - PRE-REQUEST INSPECTION ###")
        log.info(f"### Target URL: {question_url}")
        log.info(f"### Headers: {json.dumps(question_headers, indent=2)}")
        log.info(f"### JSON Payload: {json.dumps(question_data, indent=2, ensure_ascii=False)}")
        curl_command = f"""
curl -X POST \
  '{question_url}' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer {self.valves.API_KEY}' \
  -d '{json.dumps(question_data, ensure_ascii=False)}'"""
        log.info(f"### Equivalent cURL command:
{curl_command}")
        log.info("############################################################")
        # ======================================================

        try:
            async with httpx.AsyncClient(timeout=300, verify=False) as client:
                async with client.stream("POST", question_url, headers=question_headers, json=question_data) as response:
                    if response.status_code != 200:
                        error_content = await response.aread()
                        yield f"Error from RAGFlow API: {response.status_code} - {error_content.decode()}"
                        return

                    log.info("Successfully connected to RAGFlow stream. Streaming response...")
                    full_answer = ""
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            try:
                                json_data_str = line[5:].strip()
                                if not json_data_str or json_data_str == '[DONE]':
                                    continue
                                
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
                                log.error(f"Error processing stream line: {repr(e)}")
        except httpx.ConnectError as e:
            log.error(f"Connection to RAGFlow failed: {repr(e)}")
            yield f"Error: Could not connect to RAGFlow service at {self.valves.HOST}:{self.valves.PORT}"
        except Exception as e:
            log.error(f"An unexpected error occurred in pipe: {repr(e)}")
            yield f"An unexpected error occurred: {repr(e)}"
'''
