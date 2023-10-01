from typing import Optional
import lamindb_setup
import subprocess
from lamin_utils import logger, colors
import shutil
import os


# also see lamindb.dev._run_context.reinitialize_notebook for related code
def update_notebook_metadata(nb, notebook_path):
    from nbproject.dev import write_notebook
    from nbproject.dev._initialize import nbproject_id

    updated = False
    # ask for generating new id
    if os.getenv("LAMIN_TESTING") is None:
        response = input("Do you want to generate a new id? (y/n) ")
    else:
        response = "y"
    if response == "y":
        nb.metadata["nbproject"]["id"] = nbproject_id()
        updated = True
    else:
        current_version = nb.metadata["nbproject"]["version"]
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


def track(notebook_path: str, pypackage: Optional[str] = None) -> None:
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
        logger.success("attached notebook id to ipynb file")
    else:
        logger.info(f"the notebook {notebook_path} is already tracked")
        update_notebook_metadata(nb, notebook_path)
    return None


def save(notebook_path: str, **kwargs) -> Optional[str]:
    try:
        from nbproject.dev import (
            read_notebook,
            MetaStore,
            check_consecutiveness,
            MetaContainer,
        )
        from nbproject._meta import meta
        from nbproject.dev._meta_live import get_title
        import nbstripout  # noqa
    except ImportError:
        logger.error("install nbproject & nbstripout: pip install nbproject nbstripout")
        return None
    nb = read_notebook(notebook_path)  # type: ignore
    nb_meta = nb.metadata
    is_consecutive = check_consecutiveness(nb)
    if not is_consecutive:
        if "proceed_consecutiveness" in kwargs:
            decide = kwargs["proceed_consecutiveness"]
        elif meta.env == "test":
            decide = "y"
        else:
            decide = input("   Do you still want to proceed with publishing? (y/n) ")
        if decide != "y":
            logger.error("Aborted!")
            return "aborted"
    if get_title(nb) is None:
        logger.error(
            f"No title! Update & {colors.bold('save')} your notebook with a title '# My"
            " title' in the first cell."
        )
        return "no-title"
    if nb_meta is not None and "nbproject" in nb_meta:
        meta_container = MetaContainer(**nb_meta["nbproject"])
    else:
        empty = "not initialized"
        meta_container = MetaContainer(id=empty, time_init=empty, version=empty)

    meta_store = MetaStore(meta_container, notebook_path)
    import lamindb as ln

    #
    transform_version = meta_store.version
    # the corresponding transform family in the transform table
    transform_family = ln.Transform.filter(id__startswith=meta_store.id).all()
    # the specific version
    transform = transform_family.filter(version=transform_version).one()
    # latest run of this transform by user
    run = (
        ln.Run.filter(
            transform=transform, created_by__id=lamindb_setup.settings.user.id
        )
        .order_by("-run_at")
        .first()
    )
    # convert the notebook file to html
    notebook_path_html = notebook_path.replace(".ipynb", ".html")
    logger.info(f"exporting notebook as html {notebook_path_html}")
    result = subprocess.run(f"jupyter nbconvert --to html {notebook_path}")
    assert result.returncode == 0
    # copy the notebook file to a temporary file
    notebook_path_tmp = notebook_path.replace(".ipynb", "_tmp.ipynb")
    shutil.copy2(notebook_path, notebook_path_tmp)
    logger.info("stripping output of {notebook_path_tmp}")
    result = subprocess.run(f"nbstripout {notebook_path_tmp}")
    assert result.returncode == 0
    # register the html report
    initial_report = None
    initial_source = None
    if len(transform_family) > 0:
        for transform in transform_family.order_by("-created_at"):
            # check for id to avoid query
            if transform.latest_report_id is not None:
                # any previous latest report of this transform is OK!
                initial_report = transform.latest_report
            if transform.source_file_id is not None:
                # any previous source file id is OK!
                initial_source = transform.source_file
    report_file = ln.File(
        notebook_path_html,
        description=f"Report of transform {transform.id}",
        version=transform_version,
        is_new_version_of=initial_report,
    )
    report_file.save()
    run.report = report_file
    run.save()
    # register the source code
    source_file = ln.File(
        notebook_path_tmp,
        description=f"Source of transform {transform.id}",
        version=transform_version,
        is_new_version_of=initial_source,
    )
    source_file.save()
    transform.source_file = source_file
    transform.latest_report = report_file
    transform.save()
    logger.success("saved notebook and wrote source_file and html report")
    return None
