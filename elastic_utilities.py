from typing import Callable
from elasticsearch import Elasticsearch


# definisce una "struct" utile per il remapping (alias, old_index, new_index)
class ExampleReindexStruct:
    def __init__(self, alias: str, old_index: str, new_index: str) -> None:
        self.alias: str = alias
        self.old_index: str = old_index
        self.new_index: str = new_index


def get_reindexStruct_from_alias(
    es: Elasticsearch, alias: str, get_new_indexName_from_old: Callable[[str, str], str]
) -> ExampleReindexStruct:
    if not es.indices.exists_alias(name=alias):
        raise RuntimeError("Alias " + alias + " do no exist")

    old_index: str = list(es.indices.get_alias(name=alias).keys())[0]
    new_index: str = get_new_indexName_from_old(old_index, alias)

    return ExampleReindexStruct(alias, old_index, new_index)


def axpo_increment_indexNumber(old_index: str, alias: str) -> str:
    actual_index_number: str = old_index.split(alias + "-")[1]
    next_index_number: str = str(int("9" + actual_index_number) + 1)[1:]
    return alias + "-" + next_index_number


def back_to_000001(old_index: str, alias: str) -> str:
    return alias + "-000001"
