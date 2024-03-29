"""
Contains the GASearch Interface and some sample code for demonstration.

Documentation in this file follows the NumPy docstring standard for function definitions.
https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard
"""

import decimal
import logging
import random
from typing import Callable

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

    class NonexistentProjectException(Exception):
        pass

    class NoAuthTokenException(Exception):
        pass

    def __init__(self, server_url: str, project_id: str) -> None:
        """
        Initializes the local database and populates it with the project's schema and existing records.

        Parameters
        ----------
        `server_url`
            URL pointing to a deployed instance of morpho-db
        `project_id`
            UUID of a project hosted on `server_url`
        """
        self.server_url = server_url
        self.project_id = project_id
        self.db = TinyDB(f"{project_id}.json")
        self.token = None

        self.load_schema()
        self.load_records()

    def load_schema(self):
        """
        populates `schema` with a locally cached schema or fetches it from the server pointed to by `server_url`

        Raises
        ------
        NonexistentProjectException
            If the project specified by `project_id` is not found on `server_url`
        requests.exceptions.ConnectionError
            If the server specified by 'server_url` is unreachable
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
            raise self.NonexistentProjectException(
                f"Project {self.project_id} not found")

        # fetch metadata field and construct schema object
        response_data = response.json()["metadata"]
        metadata = {"schema": response_data}
        schema_table.insert(metadata)
        self.schema = MorphoProjectSchema(
            fields=schema_table.all()[0]["schema"])

    def load_records(self):
        """
        Populates `db` with records from a locally cached list of generated models or fetches them from the server pointed to by `server_url`.

        Raises
        ------
        NonexistentProjectException
            If the project specified by `project_id` is not found on `server_url`
        requests.exceptions.ConnectionError
            If the server specified by 'server_url` is unreachable

        """
        record_table = self.db.table("records")

        if len(record_table.all()) == 0:
            endpoint = f"{self.server_url}/project/{self.project_id}/model/"
            response = requests.get(endpoint)

            if response.status_code == 404:
                raise self.NonexistentProjectException(
                    f"Project {self.project_id} not found")

            response_data = response.json()
            parents = [datum["parameters"] for datum in response_data]
            record_table.insert_multiple(parents)

    def generate_child(self, fitness_query: QueryLike, sort_condition: str | None = None, sort_ascending: bool = True, limit_value: int | None = None, **kwargs: dict) -> dict | None:
        '''
        Creates a parametric child from a pool of parents either through random generation or through genetic crossover.
        Inserts the created child into `db` and also returns it for presentation purposes.

        Errors in child generation are logged in a generated file `search.log`.

        Parameters
        ----------

        fitness_query
            A `tinydb.Query` object representing a query.

        sort_condition
            field name of a field to sort by.

        sort_ascending
            specifies whether the fitness pool should be sorted in ascending order (if True) or in descending order (if False).

        limit_value
            specifies the amount of generated models to include from a sorted pool; analogue to a LIMIT SQL clause.

        kwargs
            parent_count: int
                Specifies the amount of parents the child should inherit from. Should fall in the range [0, 2], inclusive.

            dual_parent_mutation_threshold: float
                Specifies the probability of random mutation when a child inherits from 2 parents. Is 0.1 by default and should fall in the range [0, 1).

        Returns
        -------
        `record` : dict | None
            `record` is a dictionary if a child is successfully generated or `None` in the case that an error occurs during the generation process.

        Raises
        ------
        Exception
            A catchall exception. Details are stored in a local `search.log` logfile.

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
                # Breed from 2 parents

                def uniform_line(value1: float | int, value2: float | int, UNIF_SIGMA_X: float = 0.5, NORMAL_SIGMA_X: float = 0.6):
                    """
                    Returns a random value in between `value1` and `value2`, weighted by UNIF_SIGMA_X.
                    """
                    diff = abs(value1 - value2)

                    mu = (1 + UNIF_SIGMA_X * 2) * \
                        random.random() - UNIF_SIGMA_X

                    return min(value1, value2) + diff * mu

                # select 2 parents from the pool
                parent1 = random.choice(parents)
                parent2 = random.choice(parents)

                for field in self.schema.fields:
                    mutation_chance = random.random()
                    chance = random.randint(1, 2)

                    mutation_threshold = kwargs.pop(
                        "dual_parent_mutation_threshold", 0.1)
                    assert isinstance(mutation_threshold, float) or isinstance(
                        mutation_threshold, int)

                    if mutation_chance < mutation_threshold:
                        # generate a random gene for this field
                        if field.field_type == MorphoBaseType.FLOAT or field.field_type == MorphoBaseType.DOUBLE:
                            record[field.field_name] = field.field_range[0] + \
                                random.random() * \
                                (field.field_range[1] - field.field_range[0])
                        elif field.field_type == MorphoBaseType.INT:
                            record[field.field_name] = random.randint(
                                int(field.field_range[0]), int(field.field_range[1]))
                    elif chance == 2:
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
                    elif chance == 1:
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

    def get_token(self, credential_backend: Callable[[], dict[str, str]] | None = None):
        """
        Fetches and populates `token` with a cached authorization token or fetches an authorization token from the server pointed to by `server_url`.
        Requires a username, password and OTP to be provided by a backend.

        Parameters
        ----------
        credential_backend
            A function that returns a dictionary of the form {"username": "...", "password": "...", "token": "..."}. By default, this is the `get_credentials_from_cli` method.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the server specified by 'server_url` is unreachable
        """
        auth_table = self.db.table("auth_token")
        if len(auth_table.all()) > 0:
            # auth token is cached
            self.token = auth_table.all()[0]["token"]
            return

        # change token backend later
        if credential_backend is None:
            credential_backend = self.get_credentials_from_cli
        credentials = credential_backend()
        endpoint = f"{self.server_url}/token_login/"
        response = requests.post(endpoint, data=credentials)
        if (response.ok):
            response_json = response.json()
            auth_table.insert({"token": response_json["token"]})
            self.token = response_json["token"]

    def get_credentials_from_cli(self):
        """
        Basic backend to get the username, password and otp from the command line.

        Returns
        -------

        `credentials`
            A dictionary of the form {"username": "...", "password": "...", "token": "..."}.
        """
        username = input("username: ")
        password = input("password: ")
        otp = input("OTP (without spaces): ")
        return {"username": username, "password": password, "token": otp}

    def put_records(self) -> int:
        """
        Dumps pool of children from `db` to the server.

        Logs any errors to a local `search.log` logfile.

        Returns
        -------
        records_dumped
            The number of records uploaded to the server

        Raises
        ------
        NoAuthTokenException
            If the `token` field is not populated by the `get_credentials()` method.
        requests.exceptions.ConnectionError
            If the server pointed to by `server_url` is unreachable

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

        bulk_payload = [{"parameters": record}
                        for record in record_table.all()]
        response = requests.post(endpoint, headers=headers, json=bulk_payload)
        if response.ok:
            response_json = response.json()
            records_dumped = response_json["models_created"]
        elif not response.ok:
            logging.error(f"records could not be uploaded; {response.json()}")

        return records_dumped


if __name__ == "__main__":
    # sample code
    SERVER_URL, project_id = open("params.txt").read().strip().split(",")
    search_object = GASearch(SERVER_URL, project_id)
    print(search_object.generate_child(Q.step > 1))
    search_object.get_token()
    search_object.put_records()
