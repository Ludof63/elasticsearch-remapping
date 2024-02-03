from typing import Any, Mapping, List
from elasticsearch import Elasticsearch
import asyncio
import os

from remapping import remapping
import elastic_utilities as utils

CLOUD_ID = os.getenv("CLOUD_ID")
if CLOUD_ID == None:
    print("CLOUD_ID not set")
    exit(-1)

API_KEY = os.getenv("API_KEY")
if API_KEY == None:
    print("API_KEY not set")
    exit(-1)

MAPPING_COMPONENT_MORE_KEYWORDS = "example-mapping-component-more-keywords"
ILM_POLICY = "example_ilm_policy"

INDEX_SETTINGS_TEMPLATE: Mapping[str, Any] = {
    "index": {
        "lifecycle": {"name": ILM_POLICY, "rollover_alias": ""},
        "number_of_shards": "2",
    }
}


ALIAS_LIST: List[str] = [
    "dev-mulesoft-creditcheckoutbound",
    "dev-mulesoft-creditcheckinbound",
    "dev-mulesoft-creditcheckresult",
]


async def main():
    client = Elasticsearch(cloud_id=CLOUD_ID, api_key=API_KEY)
    for alias in ALIAS_LIST:
        elem = utils.get_reindexStruct_from_alias(
            client, alias, utils.axpo_increment_indexNumber
        )
        INDEX_SETTINGS_TEMPLATE["index"]["lifecycle"]["rollover_alias"] = elem.alias
        print(f"Retrieved index pattern : [{elem.alias}] -> calling Reindexing")
        await remapping(
            es=client,
            index_pattern=elem.alias,
            new_index=elem.new_index,
            old_index=elem.old_index,
            new_mapping=MAPPING_COMPONENT_MORE_KEYWORDS,
            index_settings=INDEX_SETTINGS_TEMPLATE,
            keep_old=False,
        )


if __name__ == "__main__":
    asyncio.run(main())
