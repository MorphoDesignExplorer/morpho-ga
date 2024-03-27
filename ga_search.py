import decimal
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


class HashableDict(dict):
    def __hash__(self) -> int:
        return hash(tuple(sorted(self.items())))


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
            return

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

    def generate_child(self, fitness_query: QueryLike, sort_condition: str | None = None, sort_ascending: bool = True, limit_value: int | None = None, **kwargs: dict) -> dict | None:
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

            def limit_to_precision(value: float, precision: int):
                context = decimal.Context(
                    prec=precision, rounding=decimal.ROUND_DOWN)
                decimal.setcontext(context)
                number = decimal.Decimal(value) / decimal.Decimal(1)
                return float(number)

            if len(parents) == 0 or ("parent_count" in kwargs and kwargs["parent_count"] == 0):
                # no parents in the pool, generate a new record
                for field in self.schema.fields:
                    if field.field_type == MorphoBaseType.FLOAT or field.field_type == MorphoBaseType.DOUBLE:
                        record[field.field_name] = field.field_range[0] + \
                            random.random() * \
                            (field.field_range[1] - field.field_range[0])
                    elif field.field_type == MorphoBaseType.INT:
                        record[field.field_name] = random.randint(
                            int(field.field_range[0]), int(field.field_range[1]))

                # set precision if present
                    if field.field_precision is not None:
                        record[field.field_name] = limit_to_precision(
                            record[field.field_name], field.field_precision)

            elif len(parents) == 1 or ("parent_count" in kwargs and kwargs["parent_count"] == 1):
                # mutate the parent
                # mutate each field in the parent record by +/- step
                def random_sign(): return random.choice([-1, 1])

                for field in self.schema.fields:
                    # generate field and clamp it to stay within range
                    record[field.field_name] = min(
                        field.field_range[1], max(
                            field.field_range[0],
                            parents[0][field.field_name] +
                            (random_sign())*field.field_step
                        )
                    )
                    # set precision if present
                    if field.field_precision is not None:
                        record[field.field_name] = limit_to_precision(
                            record[field.field_name], field.field_precision)
            else:
                def uniform_line(value1: float | int, value2: float | int, UNIF_SIGMA_X: float = 0.5, NORMAL_SIGMA_X: float = 0.6):
                    diff = abs(value1 - value2)

                    mu = (1 + UNIF_SIGMA_X * 2) * \
                        random.random() - UNIF_SIGMA_X

                    return min(value1, value2) + diff * mu

                # select 2 parents from the pool
                parent1 = random.choice(parents)
                parent2 = random.choice(parents)

                for field in self.schema.fields:
                    chance = random.randint(1, 2)
                    if chance == 2:
                        # breed this gene
                        if parent1[field.field_name] == parent2[field.field_name]:
                            record[field.field_name] = parent1[field.field_name]
                        else:
                            # interpolate field
                            record[field.field_name] = uniform_line(
                                parent1[field.field_name], parent2[field.field_name])
                            # clamp value to range
                            record[field.field_name] = min(
                                field.field_range[1], max(
                                    field.field_range[0], record[field.field_name]
                                ))
                    else:
                        # select parameter from one parent or the other
                        record[field.field_name] = random.choice([parent1[field.field_name],
                                                                  parent2[field.field_name]])

                    # set precision if present
                    if field.field_precision is not None:
                        record[field.field_name] = limit_to_precision(
                            record[field.field_name], field.field_precision)

        except Exception as e:
            print("child generation failed, check logs.")
            logging.error(repr(e))
            return None

        # check if the generated record is duplicate.
        pool_set = set([HashableDict(parent) for parent in parents])
        if HashableDict(record) in pool_set:
            print("duplicate child generated")
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
        auth_table = self.db.table("auth_token")
        if len(auth_table.all()) > 0:
            # auth token is cached
            self.token = auth_table.all()[0]["token"]
            return
        # change token backend later
        credentials = self.get_credentials_from_cli()
        endpoint = f"{self.server_url}/token_login/"
        response = requests.post(endpoint, data=credentials)
        if (response.ok):
            response_json = response.json()
            auth_table.insert({"token": response_json["token"]})
            self.token = response_json["token"]

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
    print(search_object.generate_child(Q.step > 1))
    search_object.get_token()
    search_object.put_records()
