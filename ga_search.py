import json
import logging
import random

import requests
import requests.status_codes
from morpho_typing import MorphoBaseType, MorphoProjectSchema
from tinydb import Query, TinyDB
from tinydb.queries import QueryLike
from tinydb.table import Document

Q = Query()


# setup logging
logging.basicConfig(filename="search.log",
                    format="%(asctime)s %(levelname)s:%(message)s")


def sort_pool(pool: list[Document], field_name: str, ascending=True):
    """
    Sorts `pool` according to a field `field` in ascending or descending order
    """
    if len(pool) > 0 and field_name not in pool[0]:
        raise KeyError(f"field {field_name} not present in parent pool.")
    return sorted(pool, key=lambda document: document[field_name], reverse=(not ascending))


class GASearch:
    """
    Represents an instance of a GA search process.
    """

    server_url: str
    """
    URL pointing to the server
    """
    project_id: str
    """
    UUID of the project being operated on
    """
    db: TinyDB
    """
    Instance of local JSON database
    """
    schema: MorphoProjectSchema
    """
    Instance of project schema fetched from the server
    """
    token: None | str
    """
    Authorization token fetched from the server
    """

    def __init__(self, server_url: str, project_id: str) -> None:
        """
        Initializes the local database and populates it with the project's schema and existing records.
        """
        self.server_url = server_url
        self.project_id = project_id
        self.db = TinyDB(f"{project_id}.json")
        self.token = None

        self.load_schema()
        self.load_records()

    def load_schema(self):
        """
        Returns a locally cached schema or fetches it from the server pointed to by `server_url`.
        """
        schema_table = self.db.table("schema")

        if len(schema_table.all()) != 0:
            self.schema = MorphoProjectSchema(
                fields=schema_table.all()[0]["schema"])

        # construct endpoint URL
        endpoint = f"{self.server_url}/project/{self.project_id}"
        response = requests.get(endpoint)

        if response.status_code == 404:
            raise Exception(f"Project {self.project_id} not found")

        # fetch metadata field and construct schema object
        response_data = response.json()["metadata"]
        metadata = {"schema": response_data}
        schema_table.insert(metadata)
        self.schema = MorphoProjectSchema(
            fields=schema_table.all()[0]["schema"])

    def load_records(self):
        """
        Returns all the records from a locally cached list of generated models or fetches them from the server pointed to by `server_url`.
        """
        record_table = self.db.table("records")

        if len(record_table.all()) == 0:
            endpoint = f"{self.server_url}/project/{self.project_id}/model/"
            response = requests.get(endpoint)

            if response.status_code == 404:
                raise Exception(f"Project {self.project_id} not found")

            response_data = response.json()
            parents = [datum["parameters"] for datum in response_data]
            record_table.insert_multiple(parents)

    def generate_child(self, fitness_query: QueryLike, sort_condition: str | None = None, sort_ascending: bool = True, limit_value: int | None = None) -> dict | None:
        '''
        Creates a parametric child from a pool of parents either through random generation or through genetic crossover.

        :returns: `record`, if a child is successfully generated or `None` in the case that an error occurs.
        :rtype: dict
        '''
        record = {}
        record_table = self.db.table("records")

        try:
            parents = record_table.search(fitness_query)
            if sort_condition is not None:
                parents = sort_pool(parents, sort_condition, sort_ascending)
            if limit_value is not None:
                parents = parents[:limit_value]

            if len(parents) == 0:
                # no parents in the pool, generate a new record
                for field in self.schema.fields:
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
                    record = random.choice([parent1, parent2])
                elif chance == 2:
                    # mutate / generate a value
                    for field in self.schema.fields:
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
        except Exception as e:
            print("child generation failed, check logs.")
            logging.error(repr(e))
            return None

        # validate generated record
        flattened_record = [record[schema_field.field_name]
                            for schema_field in self.schema.fields]
        is_valid, errors = self.schema.validate_record(flattened_record)
        if not is_valid:
            print("child generation failed, check logs.")
            logging.error(f"generated child doesn't fit schema. {errors}")
            return None

        record_table.insert(record)
        return record  # to display and other stuff

    def get_token(self):
        """
        Fetches an authorization token from the server pointed to by `server_url`.
        """
        # change token backend later
        credentials = self.get_credentials_from_cli()
        endpoint = f"{self.server_url}/token_login/"
        response = requests.post(endpoint, data=credentials)
        if (response.ok):
            self.token = response.json()["token"]

    def get_credentials_from_cli(self):
        """
        Basic backend to get the username, password and otp from the command line.
        """
        username = input("username: ")
        password = input("password: ")
        otp = input("OTP (without spaces): ")
        return {"username": username, "password": password, "token": otp}

    def put_records(self):
        """
        Dumps pool of children from `db` to the server.
        """
        endpoint = f"{self.server_url}/project/{self.project_id}/model/"
        record_table = self.db.table("records")
        if self.token is None:
            raise Exception(
                "Authorization token not present; call get_token().")
        headers = {"Authorization": f"Token {self.token}"}
        for record in record_table.all():
            rearranged_record = [record[schema_field.field_name]
                                 for schema_field in self.schema.fields]
            self.schema.validate_record(rearranged_record)
            payload = {"parameters": json.dumps(record)}
            response = requests.post(endpoint, headers=headers, data=payload)
            if not response.ok:
                logging.error(
                    f"record {record} could not be uploaded; {response.json()}")


if __name__ == "__main__":
    # sample code
    SERVER_URL, project_id = open("params.txt").read().strip().split(",")
    search_object = GASearch(SERVER_URL, project_id)
    print(search_object.generate_child(Q.step > 55))
    search_object.get_token()
    search_object.put_records()
