# this tries to load a persistent instance created in a public s3 bucket
# with s3:GetObject and s3:ListBucket policies enabled for all
# the bucket is s3://lamin-site-assets

import lamindb_setup as ln_setup


def test_load_persistent_instance():
    assert ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT is None
    ln_setup.load("testuser1/lamin-site-assets")
    assert ln_setup.settings.instance._id is not None
    assert not ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT
    ln_setup.close()
