import yaml  # type: ignore


def get_package_name() -> str:
    with open("./lamin-project.yaml") as f:
        package_name = yaml.safe_load(f)["package_name"]
    return package_name
