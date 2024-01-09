from pathlib import Path
from importlib import import_module


MODELS = {
    "core": {
        "Dataset": False,
        "File": False,
        "Transform": False,
        "Run": True,
        "User": False,
        "Storage": False,
        "Feature": False,
        "FeatureSet": False,
        "Modality": False,
        "ULabel": False,
    },
    "bionty": {
        "Species": False,
        "Gene": False,
        "Protein": False,
        "CellMarker": False,
        "Tissue": False,
        "CellType": False,
        "Disease": False,
        "CellLine": False,
        "Phenotype": False,
        "Pathway": False,
        "ExperimentalFactor": False,
        "DevelopmentalStage": False,
        "Ethnicity": False,
        "BiontySource": False,
    },
    "lamin1": {
        "ExperimentType": False,
        "Experiment": False,
        "Well": False,
        "TreatmentTarget": False,
        "Treatment": False,
        "Biosample": False,
        "Techsample": False,
    },
}


def export_database(directory):
    import lamindb_setup as ln_setup

    def export_registry(registry, directory):
        table_name = registry._meta.db_table
        df = pd.read_sql_table(table_name, ln_setup.settings.instance.db)
        df.to_parquet(directory / f"{table_name}.parquet", compression=None)

    # export data to parquet files
    print(f"\nExporting data to parquet files in: {directory}\n")
    for schema_name, models in MODELS.items():
        for model_name in models.keys():
            schema_module = import_module(f"lnschema_{schema_name}")
            registry = getattr(schema_module, model_name)
            export_registry(registry, directory)
            many_to_many_names = [field.name for field in registry._meta.many_to_many]
            for many_to_many_name in many_to_many_names:
                link_orm = getattr(registry, many_to_many_name).through
                export_registry(link_orm, directory)


if __name__ == "__main__":
    directory = Path("./lamindb_export/new/")
    response = input(f"Will export database to {directory}. Ok? (y/n)")
    if response != "y":
        quit()
    directory.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    import lamindb as ln  # noqa

    export_database(directory)
