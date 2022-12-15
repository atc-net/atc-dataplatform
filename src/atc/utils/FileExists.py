from atc.functions import init_dbutils


def file_exists(path: str):
    """
    Helper function to check whether a file or folder exists.
    """
    try:
        init_dbutils().fs.ls(path)
        return True
    except Exception:
        return False