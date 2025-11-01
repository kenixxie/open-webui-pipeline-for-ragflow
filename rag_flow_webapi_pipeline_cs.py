'''
title: RagFlow Pipeline
author: luyilong2015
date: 2025-01-28
version: 1.1
license: MIT
description: A pipeline for retrieving relevant information from a knowledge base using the RagFlow's Agent Interface.
requirements: httpx, datasets>=2.6.1, sentence-transformers>=2.2.0
"""

from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel
import httpx
import json
import logging

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
        self.sessionKV={}
        self.valves = self.Valves(
            **{
                "API_KEY": "",
                "AGENT_ID": "",
                "HOST":"",
                "PORT":""
            }
        )

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        pass

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        print(f"inlet: {__name__}")
        if self.debug:
            chat_id=body['metadata']['chat_id']
            print(f"inlet: {__name__} - chat_id:{chat_id}")
            if self.sessionKV.get(chat_id):
                self.session_id=self.sessionKV.get(chat_id)
                print(f"cache ragflow's session_id is : {self.session_id}")
            else:
                session_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/agents/{self.valves.AGENT_ID}/sessions"
                session_headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self.valves.API_KEY}'
                }
                try:
                    async with httpx.AsyncClient() as client:
                        session_response = await client.post(session_url, headers=session_headers, json={})
                        session_response.raise_for_status()
                        json_res = session_response.json()
                        self.session_id=json_res['data']['id']
                        self.sessionKV[chat_id]=self.session_id
                        print(f"new ragflow's session_id is : {self.session_id}")
                except Exception as e:
                    log.error(f"Failed to create RAGFlow session: {repr(e)}")
                    # Propagate error to avoid continuing with an invalid session
                    raise e

            print(f"inlet: {__name__} - user:")
            print(user)
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        return body

    async def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        
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

        try:
            async with httpx.AsyncClient(timeout=300) as client:
                async with client.stream("POST", question_url, headers=question_headers, json=question_data) as response:
                    if response.status_code != 200:
                        error_content = await response.aread()
                        yield f"Workflow request failed with status code: {response.status_code} - {error_content.decode()}"
                        return

                    step = 0
                    async for line in response.aiter_lines():
                        if line:
                            try:
                                if line.startswith("data:"):
                                    json_data_str = line[5:].strip()
                                    if not json_data_str or json_data_str == '[DONE]':
                                        continue
                                    
                                    json_data = json.loads(json_data_str)
                                    
                                    if 'data' in json_data and json_data['data'] is not True and 'answer' in json_data['data'] and '* is running...' not in json_data['data']['answer']:
                                        # RAGFlow's answer is cumulative, so we send only the new part.
                                        new_content = json_data['data']['answer'][step:]
                                        if new_content:
                                            yield new_content
                                            step = len(json_data['data']['answer'])

                                    elif json_data.get('data', {}).get('reference'):
                                        referenceStr = "\n\n### references\n\n"
                                        filesList = []
                                        for chunk in json_data['data']['reference']['chunks']:
                                            if chunk['document_id'] not in filesList:
                                                filename = chunk['document_name']
                                                ext = filename.split('.')[-1].strip().lower() if '.' in filename else ''
                                                referenceStr += f"\n\n - [{chunk['document_name']}]({self.valves.HOST}:{self.valves.PORT}/document/{chunk['document_id']}?ext={ext}&prefix=document)"
                                                filesList.append(chunk['document_id'])
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
