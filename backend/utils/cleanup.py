# # remove uploaded files

# from databricks.sdk.runtime import dbutils

# def cleanup_volume():
#     items = dbutils.fs.ls("/Volumes/datacraft/default/data_uploads/")

#     for item in items:
#         dbutils.fs.rm(item.path, recurse=True)