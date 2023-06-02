from typing import Optional

from lamin_logger import logger


def track(notebook_path: str, pypackage: Optional[str] = None):
    try:
        from nbproject.dev import initialize_metadata, read_notebook, write_notebook
        from nbproject.dev._initialize import nbproject_id
    except ImportError:
        logger.warning("Install nbproject! pip install nbproject")
        return None

    nb = read_notebook(notebook_path)
    if "nbproject" not in nb.metadata:
        if pypackage is not None:
            pypackage = [pp for pp in pypackage.split(",") if len(pp) > 0]  # type: ignore # noqa
        metadata = initialize_metadata(nb, pypackage=pypackage).dict()
        nb.metadata["nbproject"] = metadata
        write_notebook(nb, notebook_path)
        logger.success("Initialized the notebook metadata.")
    else:
        logger.info(f"The notebook {notebook_path} is already tracked.")

        updated = False
        response = input("Do you want to assign a new id or version? (y/n) ")

        if response != "y":
            return None
        response = input("Do you want to generate a new id? (y/n) ")
        if response != "n":
            nb.metadata["nbproject"]["id"] = nbproject_id()
            updated = True
        response = input(
            "Do you want to set a new version (e.g. '1.1')? Type 'n' for 'no'."
            " (version/n) "
        )
        if response != "n":
            new_version = input("Please type the version: ")
            nb.metadata["nbproject"]["version"] = new_version
            updated = True

        if updated:
            logger.success("Updated the notebook metadata.")
            write_notebook(nb, notebook_path)
