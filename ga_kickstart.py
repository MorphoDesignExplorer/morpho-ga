import random

import requests
import requests.status_codes
from morpho_typing import MorphoBaseType, MorphoProjectSchema
from tinydb import Query, TinyDB
from tinydb.table import Document

SERVER_URL = "http://localhost:8000/"
Q = Query()


def get_parents(project_id: str, db: TinyDB):
    """
        Caches records from a project `project_id` on the server to a local file that is pointed to by `db`.
    """
    # select parents according to a query
    # return 2 random parents from the pool

    records = db.table("records")

    if len(records.all()) == 0:
        endpoint = f"{SERVER_URL}/project/{project_id}/model/"
        response = requests.get(endpoint)

        if response.status_code == 404:
            raise Exception(f"Project {project_id} not found")

        response_data = response.json()
        parents = [datum["parameters"] for datum in response_data]
        records.insert_multiple(parents)


def generate_child(schema: MorphoProjectSchema, parents: list[Document]):
    record = {}
    if len(parents) == 0:
        # no parents in the pool, generate a new record
        for field in schema.fields:
            if field.field_type == MorphoBaseType.FLOAT or field.field_type == MorphoBaseType.DOUBLE:
                record[field.field_name] = field.field_range[0] + \
                    random.random() * \
                    (field.field_range[1] - field.field_range[0])
            elif field.field_type == MorphoBaseType.INT:
                record[field.field_name] = random.randint(
                    int(field.field_range[0]), int(field.field_range[1]))

    elif len(parents) == 1:
        # mutate the parent
        # mutate each field in the parent record by +/- step
        pass
    else:
        # select 2 parents from the pool
        parent1 = random.choice(parents)
        parent2 = random.choice(parents)

        # toss a coin to see if we select either parent, randomly generate a value or interpolate / crossover
        chance = random.randint(1, 4)
        if chance == 1 or chance == 2:
            # select either parent
            return random.choice([parent1, parent2])
        elif chance == 2:
            # mutate / generate a value
            for field in schema.fields:
                if field.field_type == MorphoBaseType.FLOAT or field.field_type == MorphoBaseType.DOUBLE:
                    record[field.field_name] = field.field_range[0] + \
                        random.random() * \
                        (field.field_range[1] - field.field_range[0])
                elif field.field_type == MorphoBaseType.INT:
                    record[field.field_name] = random.randint(
                        int(field.field_range[0]), int(field.field_range[1]))
        else:
            # interpolate
            pass
    return record


def fetch_schema(project_id, db: TinyDB):

    schema = db.table("schema")

    if len(schema.all()) != 0:
        return MorphoProjectSchema(fields=schema.all()[0]["schema"])

    # construct endpoint URL
    endpoint = f"{SERVER_URL}/project/{project_id}"
    response = requests.get(endpoint)

    if response.status_code == 404:
        raise Exception(f"Project {project_id} not found")

    # fetch metadata field and construct schema object
    response_data = response.json()["metadata"]
    metadata = {"schema": response_data}
    schema.insert(metadata)
    return MorphoProjectSchema(fields=schema.all()[0]["schema"])


def sort_pool(pool: list[Document], field_name: str, ascending=True):
    """
    Sorts `pool` according to a field `field` in ascending or descending order
    """
    if len(pool) > 0 and field_name not in pool[0]:
        raise KeyError(f"field {field_name} not present in parent pool.")
    return sorted(pool, key=lambda document: document[field_name], reverse=(not ascending))


if __name__ == "__main__":
    # define project
    project_id = "d1445161-1ac0-4f5c-b085-acf6164396e3"
    db = TinyDB(f"{project_id}.json")

    # download or get cached schema
    schema = fetch_schema(project_id, db)
    records = db.table("records")

    # get pool of parents from fitness function
    get_parents(project_id, db)
    parent_pool = records.search(Q.step < 59)
    print(sort_pool(parent_pool, 'height', False))

    # generate child
    child_record = generate_child(schema, [])  # parents)
    print(child_record)

    # cache child in local database
    # records.insert(child_record)

    # upload child to db if it is online
