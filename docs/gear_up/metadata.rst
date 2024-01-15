********
MetaData
********

Column lineage requires metadata to accurately handle case like ``select *`` or select unqualified columns in case of join.
Without metadata, SQLLineage output partially accurate column lineage.

MetaDataProvider is a mechanism sqllineage offers so that user can optionally provide metadata information to sqllineage
to improve the accuracy.

There are two MetaDataProvider implementations that sqllineage ships with. You can also build your own by extending base
class :class:`sqllineage.core.metadata_provider.MetaDataProvider`.


DummyMetaDataProvider
=====================

.. autoclass:: sqllineage.core.metadata.dummy.DummyMetaDataProvider

By default a DummyMetaDataProvider instance constructed with an empty dict will be passed to LineageRunner.
User can instantiate DummyMetaDataProvider with metadata dict of their own instead.

.. code-block:: python

    >>> from sqllineage.core.metadata.dummy import DummyMetaDataProvider
    >>> from sqllineage.runner import LineageRunner
    >>> sql1 = "insert into main.foo select * from main.bar"
    >>> metadata = {"main.bar": ["col1", "col2"]}
    >>> provider = DummyMetaDataProvider(metadata)
    >>> LineageRunner(sql1, metadata_provider=provider).print_column_lineage()
    main.foo.col1 <- main.bar.col1
    main.foo.col2 <- main.bar.col2
    >>> sql2 = "insert into main.foo select * from main.baz"
    main.foo.* <- main.baz.*

DummyMetaDataProvider is mostly used for testing purposes. The demo above shows that when there is another SQL query like
``insert into main.foo select * from main.baz``, this provider won't help because it only knows column information for
table ``main.bar``.

However, if somehow user can retrieve metadata for all the tables from a bulk process, then as long as memory allows,
it can still be used in production.


SQLAlchemyMetaDataProvider
==========================

.. autoclass:: sqllineage.core.metadata.sqlalchemy.SQLAlchemyMetaDataProvider

On the other hand, SQLAlchemyMetaDataProvider doesn't require user to provide metadata for all the tables needed at once.
It only requires database connection information and will query the database for table metadata when needed.

.. code-block:: python

    >>> from sqllineage.core.metadata.sqlalchemy import SQLAlchemyMetaDataProvider
    >>> from sqllineage.runner import LineageRunner
    >>> sql1 = "insert into main.foo select * from main.bar"
    >>> url = "sqlite:///db.db"
    >>> provider = SQLAlchemyMetaDataProvider(url)
    >>> LineageRunner(sql1, metadata_provider=provider).print_column_lineage()

As long as ``sqlite:///db.db`` is the correct source that this SQL runs on, sqllineage will generate the correct lineage.

As the name suggests, sqlalchemy is used to connect to the databases. SQLAlchemyMetaDataProvider is just a thin wrapper
on sqlalchemy ``engine``. SQLAlchemy is capable of connecting to multiple data sources with correct driver installed.

Please refer to SQLAlchemy `Dialect`_ documentation for connection information if you haven't used sqlalchemy before.


.. note::
     **SQLLineage only adds sqlalchemy library as dependency. All the drivers are not bundled, meaning user have to install
     on their own**. For example, if you want to connect to snowflake using `snowflake-sqlalchemy`_ in sqllineage, then
     you need to run

     .. code-block:: bash

        pip install snowflake-sqlalchemy


     to install the driver. After that is done, you can use snowflake sqlalchemy url like:

     .. code-block:: python

        >>> use, password, account = "<your_user_login_name>", "<your_password>", "<your_account_name>"
        >>> provider = SQLAlchemyMetaDataProvider(f"snowflake://{user}:{password}@{account}/")

     Make sure <your_user_login_name>, <your_password>, and <your_account_name> are replaced with the appropriate values
     for your Snowflake account and user.

     SQLLineage will try connecting to the data source when SQLAlchemyMetaDataProvider is constructed and throws
     MetaDataProviderException immediately if connection fails.


.. note::
     **Some drivers allow extra connection arguments.** For example, in `sqlalchemy-bigquery`_, to specify location of
     your datasets, you can pass `location` to sqlalchemy ``creation_engine`` function:

     .. code-block:: python

        >>> engine = create_engine('bigquery://project', location="asia-northeast1")

     this translates to the following SQLAlchemyMetaDataProvider code:

     .. code-block:: python

        >>> provider = SQLAlchemyMetaDataProvider('bigquery://project', engine_kwargs={"location": "asia-northeast1"})



.. _Dialect: https://docs.sqlalchemy.org/en/20/dialects/
.. _snowflake-sqlalchemy: https://github.com/snowflakedb/snowflake-sqlalchemy
.. _sqlalchemy-bigquery: https://github.com/googleapis/python-bigquery-sqlalchemy
