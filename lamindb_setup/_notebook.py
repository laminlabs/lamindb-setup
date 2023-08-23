from typing import Optional

from lamin_utils import logger


# also see lamindb.dev._run_context.reinitialize_notebook for related code
def update_notebook_metadata(nb, notebook_path):
    from nbproject.dev import write_notebook
    from nbproject.dev._initialize import nbproject_id

    current_version = nb.metadata["nbproject"]["version"]

    updated = False
    response = input("Do you want to generate a new id? (y/n) ")
    if response != "n":
        nb.metadata["nbproject"]["id"] = nbproject_id()
        updated = True
    response = input(
        f"The current version is '{current_version}' - do you want to set a new"
        " version? (y/n) "
    )
    if response == "y":
        new_version = input("Please type the version: ")
        nb.metadata["nbproject"]["version"] = new_version
        updated = True

    if updated:
        logger.save("updated notebook metadata")
        write_notebook(nb, notebook_path)


def track(notebook_path: str, pypackage: Optional[str] = None):
    try:
        from nbproject.dev import initialize_metadata, read_notebook, write_notebook
    except ImportError:
        logger.error("install nbproject: pip install nbproject")
        return None

    nb = read_notebook(notebook_path)
    if "nbproject" not in nb.metadata:
        if pypackage is not None:
            pypackage = [pp for pp in pypackage.split(",") if len(pp) > 0]  # type: ignore # noqa
        metadata = initialize_metadata(nb, pypackage=pypackage).dict()
        nb.metadata["nbproject"] = metadata
        write_notebook(nb, notebook_path)
        logger.save("attached metadata to notebook")
    else:
        logger.info(f"the notebook {notebook_path} is already tracked")
        response = input("Do you want to assign a new id or version? (y/n) ")
        if response != "y":
            return None
        update_notebook_metadata(nb, notebook_path)
