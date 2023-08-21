# this tries to load a persistent instance created with
# lamin init --storage s3://lndb-setup-ci --name persistent-instance-to-test-load

import lamindb_setup as ln_setup


def test_load_persistent_instance():
    ln_setup.load("testuser1/lamin-site-assets")
    ln_setup.close()
