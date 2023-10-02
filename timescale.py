from odoo import models, fields


class Timescale(models.AbstractModel):
    """ Main super-class for regular database-persisted Timescale Odoo models.

        Odoo models are created by inheriting from this class::

            class MySensors(Timescale):
                ...

        The system will later instantiate the class once per database (on
        which the class' module is installed).

        A "_sql_partition_key" in a database, particularly in the context of TimescaleDB,
        refers to one or more columns used to determine how data is divided or partitioned within a table.
        This partitioning is a technique to manage and organize data more efficiently,
        especially in the case of time-series data.
    """
    _partition_key = False  # refers to one or more columns used to determine how data is divided or partitioned.
    _auto = True  # automatically create database backend
    _register = False  # not visible in ORM registry, meant to be python-inherited only
    _abstract = False  # not abstract
    _transient = False  # not transient
    _sql_partition_key = None  #: SQL _sql_partition_key field_name required,

    def _check_hypertable_exist(self):
        """
           Check if a hypertable with the specified table name exists in the TimescaleDB catalog.

           This method sends a SQL query to the database to check for the existence of a hypertable
           with the given table name.

           :return: True if the hypertable exists, False otherwise.
           :rtype: bool
           """
        if self._abstract:
            return
        cr = self._cr
        query = f"""
                SELECT EXISTS (
                    SELECT 1 s
                    FROM _timescaledb_catalog.hypertable 
                    WHERE table_name = '{self._table}'
                )

            """
        cr.execute(query)
        query_fetch = cr.fetchall()
        return query_fetch[0][0]

    def _check_field_type_for_partition_key(self, field_name):
        """
        Checks if the field in the  of _sql_partition_key is of type timestamp.

        Returns:
            bool: True if the field is of type timestamp.

        Raises:
            ValueError: If the field is not of type timestamp.
        TODO: I don't know how he behaves with the inheritance
        """
        if field_name not in self._fields or field_name is not self._sql_partition_key:
            raise ValueError(f"'{field_name}' is not a valid partition key")

        field = self._fields.get(field_name)
        assert field.column_type[0] == 'timestamp', ValueError("If the field is not of type timestamp")

    def _sync_partition_key_to_database(self):
        """
        Synchronize the database hypertable's partition key with the one specified
        in _sql_partition_key.
        Returns:
            None

        Raises:
            ValueError: if _sql_partition_key does not exist or is empaty
        """
        if self._abstract:
            return

        if not self._sql_partition_key:
            raise ValueError(f"The _sql_partition_key is either missing.")

        self._check_field_type_for_partition_key(self._sql_partition_key)

        if not self._check_hypertable_exist():
            self._creat_hypertable(self._sql_partition_key)

    def _creat_hypertable(self, column):
        """
        Create a hypertable for Model,
        Args:
            columns (str): A str of column name to include in the hypertable.
        Raises:
            ValueError: If the `column` is empty or contains invalid column names.
        TODO Error: migrate_data if existing data
        """
        if self._abstract:
            return

        if not column:
            raise ValueError("The 'column' cannot be empty.")

        # Validate the column names (you may add more validation logic here)
        if not isinstance(column, str):
            raise ValueError(f"Invalid column names: {column}")

        cr = self._cr

        # info https://docs.timescale.com/api/latest/hypertable/create_hypertable/
        query = f"SELECT create_hypertable('{self._table}', '{column}');"
        cr.execute(query)
        query_fetch = cr.fetchall()

    def init(self):
        if self._abstract:
            return
        self._sync_partition_key_to_database()
