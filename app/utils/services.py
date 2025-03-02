def build_gcs_object_path(gcs_subdir, gcs_filename, file_format):
    """
    Returns string representation of 'gcs_subdir/gcs_filename.file_format'
    """
    return "/".join([gcs_subdir, ".".join([gcs_filename, file_format])])

def build_bq_table_name(bq_project_id, bq_dataset_id, bq_table_id):
    """
    Returns string representation of 'bq_project_id.bq_dataset_id.bq_table_id'
    """
    return ".".join([bq_project_id, bq_dataset_id, bq_table_id])

def build_path(list_of_path_segments):
    """
    Given a list of strings ["path", "to_this", "query"], returns 'path/to_this/query'
    """
    return '/'.join(list_of_path_segments)

def get_query(input_list):
    """
    Given a list of strings ["path", "to_this", "query"], returns the file contents of the query
    """
    path_to_query = build_path(input_list)
    return open(path_to_query, 'r').read()
