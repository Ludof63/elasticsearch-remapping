from typing import Mapping, List, Any
from elasticsearch import Elasticsearch


async def remapping(es: Elasticsearch, index_pattern: str, new_index: str, old_index: str, new_mapping: str, index_settings: Mapping[str, Any] | None = None, keep_old: bool = True, keep_template: bool = True):
    # verifico che indice (da remappare) esista
    if (not es.indices.exists(index=old_index)):
        raise RuntimeError("The old index doesn't exist")

    # creo index-template
    template_name = f'{index_pattern}-template'
    es.indices.put_index_template(name=template_name, index_patterns=index_pattern+"*", composed_of=[new_mapping])

    # se il nuovo indice esiste già lo elimino
    if (es.indices.exists(index=new_index)):
        print(f'{new_index} already existed...removing it and recreating it \t[{index_pattern}]')
        es.indices.delete(index=new_index)

    # credo il nuovo indice
    es.indices.create(index=new_index, settings=index_settings)
    print(f"Index Creation -> DONE \t[{index_pattern}]")

    # lista di azioni da eseguire su alias
    add_new_alias: Mapping[str, Any] = {
        "add": {
            "index": new_index,
            "alias": index_pattern,
            "is_write_index": True
        }
    }
    remove_old_alias: Mapping[str, Any] = {
        "remove": {
            "index": old_index,
            "alias": index_pattern
        }
    }
    alias_actions: List[Mapping[str, Any]] = [add_new_alias, remove_old_alias]

    # eseguo le azioni su alias
    es.indices.update_aliases(actions=alias_actions)
    print(f"Aliases -> DONE \t[{index_pattern}]")

    # effettuo il reindex
    reindex_source: Mapping[str, str] = {
        "index": old_index
    }
    reindex_dest: Mapping[str, str] = {
        "index": new_index
    }
    print("Reindex -> start")
    es.reindex(source=reindex_source, dest=reindex_dest)
    print(f"Reindex -> DONE \t[{index_pattern}]")

    # eliminazione indice e template se specificato
    if (not keep_old):
        es.indices.delete(index=old_index)
        print(f"Cleaning -> removed old index \t[{index_pattern}]")
    if (not keep_template):
        es.indices.delete_index_template(name=template_name)
        print(f"Cleaning -> removed template \t[{index_pattern}]")
