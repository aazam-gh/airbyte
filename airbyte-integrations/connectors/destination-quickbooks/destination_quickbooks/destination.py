#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status
import requests
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session


class DestinationQuickbooks(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        pass

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:

            client_id = config['client_id']
            client_secret = config['client_secret']
            redirect_uri = config['redirect_uri']
            refresh_token = config['refresh_token']
            access_token = config['access_token']
            realm_id = config['realm_id']
            
            authorization_base_url = 'https://appcenter.intuit.com/connect/oauth2'
            token_url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
            scope = ['com.intuit.quickbooks.accounting']

             # Define the OAuth 2.0 session
            intuit = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
            # Get the authorization URL
            authorization_url, state = intuit.authorization_url(authorization_base_url)
            print('Please go to', authorization_url)
            authorization_response = input('Paste the full redirect URL here: ')
            # Fetch the access token
            intuit.fetch_token(token_url, authorization_response=authorization_response, auth=HTTPBasicAuth(client_id, client_secret))
            ####End
                
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
