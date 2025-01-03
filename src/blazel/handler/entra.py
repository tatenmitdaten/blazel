import gzip
import logging
import os
import re
from dataclasses import dataclass
from dataclasses import field
from functools import lru_cache
from io import BufferedReader
from pathlib import Path
from typing import ClassVar
from typing import Generator

import msal  # type: ignore
import requests

from blazel.clients import get_secretsmanager_client

DictTuple = tuple[tuple[str, str], ...]

logger = logging.getLogger()


def is_lambda_runtime():
    return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None


@dataclass
class EntraServiceHandler:
    base_url: ClassVar[str]  # API base url
    scopes: ClassVar[list[str]]  # API scopes
    access_token: str | None = field(default=None, init=False)  # MSAL token

    secret_id: str  # AWS Secrets Manager secret id for MSAL token
    tenant_id: str  # Entra tenant id
    client_id: str  # Entra app registration

    @property
    def authority(self):
        return f'https://login.microsoftonline.com/{self.tenant_id}'

    @property
    def headers(self) -> dict:
        return dict(self.headers_tuple)

    @property
    def headers_tuple(self) -> DictTuple:
        return (
            ('Authorization', f"Bearer {self.token}"),
            ('Content-Type', 'application/json')
        )

    def init_token_cache(self) -> msal.SerializableTokenCache:
        if is_lambda_runtime():
            raise RuntimeError('Cannot run device flow in Lambda.')
        token_cache = msal.SerializableTokenCache()
        app = msal.PublicClientApplication(self.client_id, authority=self.authority, token_cache=token_cache)
        flow = app.initiate_device_flow(self.scopes)
        message = 'Code: {user_code} Url: {verification_uri}'.format(**flow)
        print(message)
        app.acquire_token_by_device_flow(flow)
        secret_string = token_cache.serialize()
        client = get_secretsmanager_client()
        try:
            client.put_secret_value(
                SecretId=self.secret_id,
                SecretString=secret_string
            )
        except client.exceptions.ResourceNotFoundException:
            client.create_secret(
                Name=self.secret_id,
                SecretString=secret_string
            )
        logger.info(f'Wrote token cache to secret {self.secret_id}')
        return token_cache

    def get_token_cache(self) -> msal.SerializableTokenCache | None:
        client = get_secretsmanager_client()
        try:
            secret = client.get_secret_value(SecretId=self.secret_id)
            if secret['SecretString'] == '{}':
                raise ValueError('Empty secret')
        except (client.exceptions.ResourceNotFoundException, ValueError):
            return self.init_token_cache()
        token_cache = msal.SerializableTokenCache()
        token_cache.deserialize(secret['SecretString'])
        return token_cache

    @property
    def token(self) -> str | None:
        """
        Access token for Microsoft Entra services
        """
        if self.access_token:
            return self.access_token

        app = msal.PublicClientApplication(
            self.client_id,
            authority=self.authority,
            token_cache=self.get_token_cache()
        )
        accounts = app.get_accounts()
        if not accounts:
            raise RuntimeError('No accounts found in token cache.')
        token = app.acquire_token_silent(self.scopes, account=accounts[0])
        self.access_token = token['access_token']
        return self.access_token


@dataclass
class PowerBIHandler(EntraServiceHandler):
    base_url = 'https://api.powerbi.com/v1.0/myorg'
    scopes = [
        'https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All',
    ]

    def refresh_dataset(self, dataset_id: str) -> dict | None:
        url = f"{self.base_url}/datasets/{dataset_id}/refreshes"
        response = requests.post(url, headers=self.headers)
        message = {}
        if response.status_code == 202:
            logger.info(f"Dataset refresh started successfully. Dataset: {dataset_id}")
        else:
            logger.info(f"Failed to start dataset refresh. Code: {response.status_code}")
            logger.info(response.text)
            message = response.json()
        return {
            'status_code': response.status_code,
            **message
        }


@dataclass
class SharepointHandler(EntraServiceHandler):
    scopes: ClassVar[list[str]] = [
        'https://graph.microsoft.com/Files.ReadWrite.All',
        'https://graph.microsoft.com/Sites.Selected',
        'https://graph.microsoft.com/User.Read',
    ]
    base_url: ClassVar[str] = 'https://graph.microsoft.com/v1.0'
    """
    The site id is not part of the Sharepoint URL. To find the site id:
    - Navigate to the Sharepoint site and open the developer tools (F12).
    - Go to Console and enter: _spPageContextInfo.siteId
    """
    site_id: str  # Sharepoint site id

    @property
    def root_url(self):
        return f'{self.base_url}/sites/{self.site_id}/drive'

    def search_download_url(self, file_name: str) -> str:
        url = f'{self.base_url}/sites/{self.site_id}/drive/root/children'
        response = requests.get(url, headers=self.headers)
        for file in response.json()['value']:
            if file['name'] == file_name:
                logging.getLogger().debug(file)
                return file['@microsoft.graph.downloadUrl']
        raise RuntimeError(f'{file_name} not found on Sharepoint')

    @staticmethod
    def match_wildcard(file_expr: str, file_name: str) -> bool:
        """
        Match a file name against a wildcard expression.
        """
        escaped_expr = re.escape(file_expr).replace(r'\*', '.*')
        pattern = f'^{escaped_expr}$'
        return re.match(pattern, file_name) is not None

    def get_file_streams(self, file_path: str, file_expr: str) -> Generator[BufferedReader, None, None]:
        urls = self.get_download_urls_cached(self.headers_tuple, self.root_url, file_path)
        for file_name, url in urls:
            if self.match_wildcard(file_expr, file_name):
                yield self.get_file_stream_cached(urls, file_path, file_name)

    @staticmethod
    @lru_cache
    def get_download_urls_cached(headers: DictTuple, root_url: str, path: str) -> DictTuple:
        response = requests.get(f'{root_url}/root:/{path}', headers=dict(headers))
        folder_id = response.json()['id']
        response = requests.get(f'{root_url}/items/{folder_id}/children', headers=dict(headers))
        return tuple(
            (file['name'], file['@microsoft.graph.downloadUrl'])
            for file in response.json()['value']
            if '@microsoft.graph.downloadUrl' in file
        )

    @staticmethod
    @lru_cache
    def get_file_stream_cached(
            urls_tuple: DictTuple,
            file_path: str,
            file_name: str,
            local_cache_folder: str = 'cache',
            use_local_cache: bool = False
    ) -> BufferedReader:
        urls = dict(urls_tuple)

        def get_file_response(_file_path, _file_name) -> requests.Response:
            if _file_name not in urls:
                raise ValueError(f'File not found: "{_file_name}" in {urls.keys()}')
            logger.info(f'Downloading {_file_name} from {urls[_file_name]}')
            response = requests.get(urls[_file_name], stream=True)
            if response.status_code != 200:
                raise Exception(f'Error downloading file: {response.status_code}')
            return response

        stream: BufferedReader
        if use_local_cache:
            local_path = Path(local_cache_folder) / file_path / file_name
            if not local_path.exists():
                file_bytes = get_file_response(file_path, file_name).content
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(file_bytes)
            else:
                file_bytes = local_path.read_bytes()
            stream = BufferedReader(file_bytes)  # type: ignore
        else:
            file_response = get_file_response(file_path, file_name)
            if file_response.headers.get('Content-Encoding') == 'gzip':
                stream = gzip.GzipFile(fileobj=file_response.raw)  # type: ignore
            else:
                stream = BufferedReader(file_response.raw)  # type: ignore
        return stream
