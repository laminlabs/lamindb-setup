from typing import Optional

from lamin_logger import logger


def track(notebook_path: str, pypackage: Optional[str] = None):
    try:
        from nbproject.dev import initialize_metadata, read_notebook, write_notebook
    except ModuleNotFoundError:
        logger.warning("Install nbproject! pip install nbproject")
        return None

    nb = read_notebook(notebook_path)
    if "nbproject" not in nb.metadata:
        if pypackage is not None:
            pypackage = [pp for pp in pypackage.split(",") if len(pp) > 0]  # type: ignore # noqa
        metadata = initialize_metadata(nb, pypackage=pypackage).dict()
        nb.metadata["nbproject"] = metadata
        write_notebook(nb, notebook_path)
    else:
        logger.info(f"The notebook {notebook_path} is already tracked.")
