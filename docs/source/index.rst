Morpho Design Explorer - GA Client
==================================

This is the GA Client Component of the Morpho Design Explorer Project. This client provides the `GASearch` class interface to connect to a MorphoDesignExplorer backend, load project schemas and existing generated models, and generate new children with custom fitness functions.

Sample usage is as follows:

.. code-block:: python

    # Import the search interface class
    from morpho_ga import GASearch

    # Define the server location URL and the project identifier for the project to operate on
    SERVER_URL, project_id = "server_url_here", "project_id here"

    # build the search interface and load generated models from SERVER_URL
    search_object = GASearch(SERVER_URL, project_id)

    # generate a child with the following fitness function: 2 <= step < 7
    print(search_object.generate_child((Q.step >= 2) & (Q.step < 7)))

    # get auth token from the server; will trigger a CLI prompt for a username, password and OTP
    search_object.get_token()

    # dump all generated models to the server in bulk
    search_object.put_records()

The `generate_child()` method generates one child at a time. This offers great flexibility in generating a varying number of children with different fitness functions. An example is given below to demonstrate the same:

.. code-block:: python

    iterations = 0
    unique = 0
    while True:
        if unique == 10:
            break
        if unique < 5:
            # generate 5 children with random genes, from the pool with `step` > 0
            child = search_object.generate_child(Q.step > 0, parent_count=0)
        elif unique >= 5:
            # generate 5 more random chilren with genes crossed over from 2 parents, with 2 <= step < 7
            child = search_object.generate_child((Q.step >= 2) & (Q.step < 7), parent_count=0)
        if child is not None:
            # bump unique if child is not a duplicate
            unique += 1
        iterations += 1

    search_object.put_records()

References
**********

#. Paragen System Reference: https://www.ingentaconnect.com/content/iass/jiass/2012/00000053/00000004/art00010

Submodules
----------

.. toctree::
    :maxdepth: 2

    ga_search

