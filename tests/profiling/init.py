def main():
    from lamindb_setup import init

    init()


def cleanup():
    from lamindb_setup import delete

    delete("lamindb-setup", force=True)


if __name__ == "__main__":
    main()
