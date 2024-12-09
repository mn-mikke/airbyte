from pydantic import BaseModel, Field

class H2OGPTEConfigModel(BaseModel):
    address: str = Field(
        ...,
        title="Address",
        description="Address of H2OGPTE instance",
        order=1,
        examples=["https://playground.h2ogpte.h2o.ai/", ])
    api_key: str = Field(
        ...,
        title="API Key",
        description="API Key for H2OGPTE instance",
        airbyte_secret=True,
        order=2)
    collection_id: str = Field(..., title="Collection Id", description="UUID of the target collection", order=3)
