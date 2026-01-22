import os
import re
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

def create_delta_tables(folder_path: str, target_schema: str = "datacraft.default") -> str:
    """
    Reads files from a Unity Catalog volume folder and creates Delta tables in the specified schema.
    """
    try:
        files = dbutils.fs.ls(folder_path)

        for file in files:
            name = os.path.basename(file.path).split("/")[-1].split(".")[0].lower()
            ext = file.name.split(".")[-1].lower()

            if ext == "csv":
                df = spark.read.option("header", True).csv(file.path)

            elif ext in ["xls", "xlsx"]:
                df = spark.read \
                    .format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(file.path)

            else:
                print(f"Skipped unsupported file: {file.name}")
                continue

            # Create Delta table in UC
            table_name = f"{target_schema}.{name}"
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"Created table: {table_name}")

        return "All tables created successfully."

    except Exception as e:
        return f"Error: {e}"


def strip_think_parser(raw_text: str) -> str:
    """Remove all <think>...</think> blocks and return the remaining text."""
    return re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()
