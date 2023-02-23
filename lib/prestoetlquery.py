import os
import getpass

class PrestoETLQuery():
    def __init__(self):
        # This is to define how to map YAML column definitions to database
        self.columns_mapping = {
            'string' : 'varchar',
            'int' : 'integer',
            'bigint' : 'bigint',
            'timestamp' : 'timestamp',
            'date' : 'date',
            'float' : 'double',
            'double' : 'double',
            'boolean' : 'boolean',
        }

        # Get current unix username
        #username = os.getlogin()
        username = getpass.getuser

        # Default columns DDL structure
        self.default_columns_type = {
            'dw_create_ts' : {
                'type': "timestamp", 'nullable': False, 'pii': False,
                'description': "dw_create_ts",
                'etl': "now()"
                },
            'dw_create_user' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dw_create_user",
                'etl': "'{u}'".format(u=username)
                },
            'dw_modified_ts' : {
                'type': "timestamp", 'nullable': False, 'pii': False,
                'description': "dw_modified_ts",
                'etl': "now()"
                },
            'dw_modified_user' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dw_modified_user",
                'etl': "'{u}'".format(u=username)
                },
            'dl_partition_year' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dl_partition_year",
                'etl': "':current_year'"
                },
            'dl_partition_month' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dl_partition_month",
                'etl': "':current_month'"
                },
            'dl_partition_day' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dl_partition_day",
                'etl': "':current_day'"
                },
            'dl_partition_hour' : {
                'type': "string", 'nullable': False, 'pii': False,
                'description': "dl_partition_hour",
                'etl': "':current_hour'"
                },
        }

        # Default columns for ETL
        self.default_columns_etl = "\n"
        self.default_columns_list = ""
        for column in self.default_columns_type:
            self.default_columns_etl += "              ,{ct} AS {cn}\n".format(
                ct=self.default_columns_type[column]['etl'],
                cn=column,
                )
            self.default_columns_list += ",{cn}".format(cn=column)

    def gen_create_table(self, table_name, columns, partition_keys=None):
        ''' For generating create table '''
        # IF NOT EXISTS not working with Python module
        #sql_start = "CREATE TABLE IF NOT EXISTS {t} (".format(
        sql_start = "CREATE TABLE {t} (".format(
            t=table_name)
        sql_end = ") WITH (format = 'PARQUET');"
        if partition_keys is None:
            partition_keys="dl_partition_year,dl_partition_month,dl_partition_day,dl_partition_hour"
        sql_end = ") WITH (\n"
        sql_end += "       format = 'PARQUET',\n"
        sql_end += "       partitioned_by = array["
        sql_end += "'" + partition_keys.replace(",", "','") + "'"
        sql_end += "]\n"
        sql_end += "            )"
        sql_end += ";"
        sql_columns = ""

        # Add default columns to existing DDL
        columns.append(self.default_columns_type)

        for column in columns:
            for key, value in column.items():
                column_name = key
                column_type = value['type']
                column_null = ''
                # Default is to allow column to be nullable
#                if value.get('nullable', True):
#                    column_null = 'DEFAULT NULL'
#                else:
#                    column_null = 'NOT NULL'
                    
                # This is to use mapping to re-map column types
                column_type = self.columns_mapping[column_type]
                sql_columns += """
                    {cn} {ct} {null},""".format(
                    cn=column_name,
                    ct=column_type,
                    null=column_null,
                    )

        sql = """
            {start}
            {cols}

            {end}
        """.format(
            start=sql_start,
            cols=sql_columns[:-1],
            end=sql_end
            )
        
        return sql

    def gen_etl_tmp(self, table_name, sql):
        ''' For generating TMP table ETL step '''
        sql_start = """
            DROP TABLE IF EXISTS {tmp};
            CREATE TABLE {tmp} AS
        """.format(
            tmp=table_name)

        sql_end = ";"

        sql_etl = """
            {start}
            {etl}
            {end}
        """.format(
            start=sql_start,
            etl=sql,
            end=sql_end,
            )
        
        return sql_etl

    def gen_etl_stg(self, table_name, sql, delete_clause=None):
        ''' For generating STAGING table ETL step '''
        if delete_clause is not None:
            sql_delete = "DELETE FROM {t} WHERE {d};".format(
                t=table_name, d=delete_clause)

        sql_start = "INSERT INTO {t}".format(
            t=table_name)

        sql_end = ";"

        sql_etl = """
            {delete}
            {start}
            {etl}
            {end}
        """.format(
            delete=sql_delete,
            start=sql_start,
            etl=sql,
            end=sql_end,
            )
        
        return sql_etl

    def gen_etl_tgt_overwrite(self, target_table, source_table, target_columns):
        ''' For generating Target table ETL step for OVERWRITE mode '''
        sql_start = """
            DELETE FROM {tgt}
            ;
        """.format(tgt=target_table)

        sql_etl = """
            INSERT INTO {tgt}
            ({all_cols})
            SELECT
              {t_cols}
              {d_cols}
            FROM {src}
        """.format(
            tgt=target_table, src=source_table, all_cols=target_columns+self.default_columns_list,
                t_cols=target_columns, d_cols=self.default_columns_etl)

        sql_end = ";"

        sql = """
            {start}
            {etl}
            {end}
        """.format(
            start=sql_start,
            etl=sql_etl,
            end=sql_end,
            d_ols=self.default_columns_etl,
            )

        return sql

    def gen_etl_tgt_update(self, target_table, source_table, target_columns, primary_key):
        ''' For generating Target table ETL step for UPDATE mode '''
        # Intermediate tmp table for table swap
        tmp_table = "{tgt}__tmp".format(
            c=self.catalog, tgt=target_table)

        sql_start = """
            DROP TABLE IF EXISTS {tmp}
            ;
        """.format(
            tmp=tmp_table, src=source_table)

        sql_etl = """
            CREATE TABLE {tmp} AS
            SELECT
              {t_cols}
              {d_cols}
            FROM {src}
            ;
            INSERT INTO {tmp}
            SELECT
              {t_cols}
              {d_cols}
            FROM {tgt}
              WHERE ({pk}) NOT IN (SELECT {pk} FROM {src})
            ;
        """.format(
            tmp=tmp_table, tgt=target_table, src=source_table, pk=primary_key,
                t_cols=target_columns, d_cols=self.default_columns_etl)

        sql_end = """
            DROP TABLE {tgt};
            CREATE TABLE {tgt} AS
            SELECT * FROM {tmp};
            DROP TABLE {tmp}
            ;
        """.format(
            tgt=target_table, tmp=tmp_table)

        sql = """
            {start}
            {etl}
            {end}
        """.format(
            start=sql_start,
            etl=sql_etl,
            end=sql_end,
            )

        return sql

    def gen_etl_tgt_append(self, target_table, source_table, target_columns, delete_clause=None):
        ''' For generating Target table ETL step for APPEND mode '''
        # Default delete clause
        if delete_clause is None:
            delete_clause="""
              dl_partition_year = ':current_year'
              AND dl_partition_month = ':current_month'
              AND dl_partition_day = ':current_day'
              AND dl_partition_hour = ':current_hour'
            """

        sql_start = """
            DELETE FROM {tgt} WHERE {d}
            ;
        """.format(
            tgt=target_table, d=delete_clause)

        sql_etl = """
            INSERT INTO {tgt}
            ({all_cols})
            SELECT
              {t_cols}
              {d_cols}
            FROM {src}
        """.format(
            tgt=target_table, src=source_table, all_cols=target_columns+self.default_columns_list,
                t_cols=target_columns, d_cols=self.default_columns_etl)
        sql_end = ";"

        sql = """
            {start}
            {etl}
            {end}
        """.format(
            start=sql_start,
            etl=sql_etl,
            end=sql_end,
            )

        return sql

    def gen_drop_tmp_tables(self, tmp_tables):
        ''' For generating drop TMP tables statements '''
        sql = "\n"
        for etl_name in tmp_tables:
            sql += "            DROP TABLE IF EXISTS {t};\n".format(t=tmp_tables[etl_name]['table'])

        return sql
