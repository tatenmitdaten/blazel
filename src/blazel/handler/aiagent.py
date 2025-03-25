import abc
import json

import boto3
import google.genai  # type: ignore
import mistralai
from botocore.client import BaseClient
from botocore.config import Config
from google.genai import types


def aws_session() -> boto3.Session:
    return boto3.Session(profile_name='default')


class ChatModel(abc.ABC):
    model_id: str
    region_name: str

    @abc.abstractmethod
    def list_models(self) -> list[str]:
        pass

    @abc.abstractmethod
    def invoke(self, text: str, max_tokens: int = 5000, sytem_prompt: str | None = None) -> str:
        pass


class Claude(ChatModel):
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    region_name = 'eu-central-1'
    config = Config(
        connect_timeout=120,
        read_timeout=120,
        retries={'max_attempts': 2}
    )

    @property
    def client(self) -> BaseClient:
        return aws_session().client(
            service_name='bedrock-runtime',
            region_name=self.region_name,
            config=self.config
        )

    def list_models(self) -> list[str]:
        client = aws_session().client(
            service_name='bedrock',
            region_name=self.region_name
        )
        response = client.list_foundation_models(byProvider='anthropic')
        return sorted(m['modelId'] for m in response['modelSummaries'])

    def invoke(self, text: str, max_tokens: int = 5000, system_prompt: str | None = None) -> str:
        client = aws_session().client(
            service_name='bedrock-runtime',
            region_name=self.region_name,
            config=self.config
        )
        messages = []
        if system_prompt:
            messages.append({
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": system_prompt
                    }
                ]
            })
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": messages + [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": text
                        }
                    ]
                }
            ]
        }
        try:
            response = client.invoke_model(modelId=self.model_id, body=json.dumps(body))
            response_body = json.loads(response['body'].read())
            response_text = response_body['content'][0]['text']
        except Exception as e:
            print(f"Error {type(e)}: {str(e)}")
            raise
        return response_text


class Gemini(ChatModel):
    model_id = 'gemini-2.0-flash'

    @property
    def client(self) -> google.genai.Client:
        response = aws_session().client('ssm').get_parameter(
            Name='/google/genai-api-key',
            WithDecryption=True
        )
        api_key = response['Parameter']['Value']
        return google.genai.Client(api_key=api_key)

    def list_models(self) -> list[str]:
        return sorted(m.name for m in self.client.models.list())

    def invoke(self, text: str, max_tokens: int = 5000, system_prompt: str | None = None) -> str:
        response = self.client.models.generate_content(
            model=self.model_id,
            contents=text,
            config=types.GenerateContentConfig(
                max_output_tokens=max_tokens,
                temperature=0.1,
                system_instruction=system_prompt
            )
        )
        return response.text


class Mistral(ChatModel):
    model_id = "mistral-large-latest"

    @property
    def client(self) -> mistralai.Mistral:
        response = aws_session().client('ssm').get_parameter(
            Name='/mistral/blazel-api-key',
            WithDecryption=True
        )
        api_key = response['Parameter']['Value']
        return mistralai.Mistral(api_key=api_key)  # type: ignore

    def list_models(self) -> list[str]:
        with self.client as client:
            return sorted(m.id for m in client.models.list().data)

    def invoke(self, text: str, max_tokens: int = 5000, system_prompt: str | None = None) -> str:
        messages = []
        if system_prompt:
            messages.append({
                "role": "system",
                "content": system_prompt
            })
        messages.append({
            "role": "user",
            "content": text
        })
        with self.client as client:
            response = client.chat.complete(
                model=self.model_id,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.1,
            )
            return response.choices[0].message.content
