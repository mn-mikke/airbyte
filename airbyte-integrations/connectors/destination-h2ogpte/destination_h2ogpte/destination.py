#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination

from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteLogMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    Type,
    Level
)
from h2ogpte import H2OGPTE
from .config import H2OGPTEConfigModel
import tempfile
import os


class DestinationH2OGPTE(Destination):
    def __init__(self):
        self._temp_file = None
        self._filename = None

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        parsed_config = H2OGPTEConfigModel.parse_obj(config)

        for message in input_messages:
            if message.type == Type.STATE:
                if self._temp_file:
                    client = H2OGPTE(
                        address=parsed_config.address,
                        api_key=parsed_config.api_key,
                    )
                    with open(self._filename, 'rb') as f:
                        upload_id = client.upload(self._filename + ".txt", f)
                    client.ingest_uploads(parsed_config.collection_id, [upload_id, ])
                    os.close(self._temp_file)
                    self._temp_file = None
                    self._filename = None
                yield message
            elif message.type == Type.RECORD:
                if not self._temp_file:
                    self._temp_file, self._filename = tempfile.mkstemp()
                with open(self._filename, 'a') as f:
                    f.write(str(message.record.data))
                yield AirbyteMessage(type=Type.LOG, log=AirbyteLogMessage(level=Level.INFO, message="stored message"))

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        parsed_config = H2OGPTEConfigModel.parse_obj(config)
        client = H2OGPTE(
            address=parsed_config.address,
            api_key=parsed_config.api_key,
        )
        try:
            client.count_collections()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message="Error with exception:" + str(e))

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="TODO",
            supportsIncremental=True,
            supported_destination_sync_modes=[DestinationSyncMode.append],
            connectionSpecification=ConfigModel.schema(),  # type: ignore[attr-defined]
        )
