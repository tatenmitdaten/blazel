from functools import lru_cache
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import botocore.errorfactory
import msal  # type: ignore
import requests

from blazel.clients import get_secretsmanager_client

local_cache_folder = 'cache'
use_local_cache = False
DictTuple = tuple[tuple[str, str], ...]


@dataclass
class SharepointHandler:
    tenant_id: str  # Azure AD tenant id
    client_id: str  # Azure AD app registration
    site_id: str  # Sharepoint site id
    secret_id: str  # AWS Secrets Manager secret id for MSAL token
    access_token: str | None = None  # MSAL token
    scopes: list[str] | tuple[str, ...] = (
        'https://graph.microsoft.com/Files.ReadWrite.All',
        'https://graph.microsoft.com/Sites.Selected',
        'https://graph.microsoft.com/User.Read',
    )
    base_url: str = 'https://graph.microsoft.com/v1.0'

    @property
    def authority(self):
        return f'https://login.microsoftonline.com/{self.tenant_id}'

    @property
    def root_url(self):
        return f'{self.base_url}/sites/{self.site_id}/drive'

    @property
    def headers(self) -> dict:
        return dict(self.header_tuple)

    @property
    def header_tuple(self) -> DictTuple:
        return (
            ('Authorization', f"Bearer {self.get_token()}"),
            ('Content-Type', 'application/json')
        )

    def get_token(self) -> str:
        """
        Get an access token from Azure AD using MSAL and cache it in AWS Secrets Manager

        Returns:
            access token
        """
        if self.access_token is not None:
            return self.access_token

        token = None  # MSAL token
        secret = None  # secret from secret store
        secret_client = get_secretsmanager_client()
        token_cache = msal.SerializableTokenCache()

        # try to load token cache from secret store
        try:
            secret = secret_client.get_secret_value(SecretId=self.secret_id)
        except botocore.errorfactory.ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                pass  # Secret has not been created yet
            else:
                raise

        if secret:
            token_cache.deserialize(secret['SecretString'])

        # create a public client app using the token cache
        app = msal.PublicClientApplication(self.client_id, authority=self.authority, token_cache=token_cache)
        accounts = app.get_accounts()  # get accounts from token cache
        if accounts:
            token = app.acquire_token_silent(list(self.scopes), account=accounts[0])

        if token is None:  # token not in cache or expired
            if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                raise RuntimeError('The refresh token expired. Run the device flow on your local machine.')

            # initiate device flow and acquire token
            flow = app.initiate_device_flow(list(self.scopes))
            print('Code: {user_code} Url: {verification_uri}'.format(**flow))
            token = app.acquire_token_by_device_flow(flow)

            # save token cache to secret store
            secret_string = token_cache.serialize()
            if secret:
                secret_client.put_secret_value(SecretId=self.secret_id, SecretString=secret_string)
            else:
                secret_client.create_secret(Name=self.secret_id, SecretString=secret_string)
            print('Wrote token_cache to secret store')
        self.access_token = token['access_token']
        return self.access_token

    def search_download_url(self, file_name: str) -> str:
        url = f'{self.base_url}/sites/{self.site_id}/drive/root/children'
        response = requests.get(url, headers=self.headers)
        for file in response.json()['value']:
            if file['name'] == file_name:
                logging.getLogger().debug(file)
                return file['@microsoft.graph.downloadUrl']
        raise RuntimeError(f'{file_name} not found on Sharepoint')

    def get_file(self, file_path: str, file_name: str) -> bytes:
        urls = self.get_download_urls_cached(self.header_tuple, self.root_url, file_path)
        return self.get_file_cached(urls, file_path, file_name)

    @staticmethod
    @lru_cache
    def get_download_urls_cached(headers: DictTuple, root_url: str, path: str) -> DictTuple:
        response = requests.get(f'{root_url}/root:/{path}', headers=dict(headers))
        print(response.json())
        folder_id = response.json()['id']
        response = requests.get(f'{root_url}/items/{folder_id}/children', headers=dict(headers))
        return tuple(
            (file['name'], file['@microsoft.graph.downloadUrl'])
            for file in response.json()['value']
        )

    @staticmethod
    @lru_cache
    def get_file_cached(urls_tuple: DictTuple, file_path: str, file_name: str) -> bytes:
        urls = dict(urls_tuple)

        def get_file(_file_path, _file_name):
            if _file_name not in urls:
                raise ValueError(f'File not found: "{_file_name}" in {urls.keys()}')
            response = requests.get(urls[_file_name])
            if response.status_code != 200:
                raise Exception(f'Error downloading file: {response.status_code}')
            return response.content

        if use_local_cache:
            local_path = Path(local_cache_folder) / file_path / file_name
            if not local_path.exists():
                file_bytes = get_file(file_path, file_name)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(file_bytes)
            else:
                file_bytes = local_path.read_bytes()
        else:
            file_bytes = get_file(file_path, file_name)
        return file_bytes
