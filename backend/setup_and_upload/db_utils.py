import databricks.sql as sql


def get_databricks_connection(token):
    return sql.connect(
        server_hostname="dbc-83de24b5-b7ed.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/0069c79611dc9f7e",
        access_token=token
    )


def execute_statements(statements, token):
    connection = None
    cursor = None
    try:
        connection = get_databricks_connection(token)
        cursor = connection.cursor()

        for stmt in statements:
            cursor.execute(stmt)

        return {"success": True, "error": None}

    except Exception as e:
        return {"success": False, "error": str(e)}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_catalog(catalog_name, token):
    statements = [
        f"CREATE CATALOG IF NOT EXISTS {catalog_name}",
        f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `account users`"
    ]
    return execute_statements(statements, token)


def create_schema(catalog_name, schema_name, token):
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}",
        f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.{schema_name} TO `account users`"
    ]
    return execute_statements(statements, token)


def create_volume_in_schema(catalog_name, schema_name, volume_name, token):
    statements = [
        f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}",
        f"GRANT ALL PRIVILEGES ON VOLUME {catalog_name}.{schema_name}.{volume_name} TO `account users`"
    ]
    return execute_statements(statements, token)
