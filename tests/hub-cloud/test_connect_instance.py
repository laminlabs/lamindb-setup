from __future__ import annotations

import os

import lamindb_setup as ln_setup
import pytest
from lamindb_setup._connect_instance import InstanceNotFoundError
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core._hub_crud import update_instance
from laminhub_rest.core.legacy._instance_collaborator import InstanceCollaboratorHandler
from postgrest.exceptions import APIError


def test_connect_pass_none():
    with pytest.raises(ValueError) as err:
        ln_setup.connect(_test=True)
    assert (
        err.exconly()
        == "ValueError: No instance was connected through the CLI, pass a value to `instance` or connect via the CLI."
    )


# do not call hub if the owner is set to anonymous
def test_connect_anonymous_owned_instance_from_hub():
    with pytest.raises(InstanceNotFoundError) as error:
        ln_setup.connect("anonymous/random-instance-not-exists")
    assert error.exconly().endswith(
        "It is not possible to load an anonymous-owned instance from the hub"
    )


def test_connect_after_revoked_access():
    # can't currently test this on staging as I'm missing the accounts
    if os.getenv("LAMIN_ENV") == "prod":
        ln_setup.login("testuser1@lamin.ai")
        admin_hub = connect_hub_with_auth()
        collaborator_handler = InstanceCollaboratorHandler(admin_hub)
        try:
            # if a previous test run failed, this will
            # error with a violation of a unique constraint
            collaborator_handler.add_by_slug(
                "testuser1/static-test-instance-private-sqlite",
                "testuser2",
                "write",
                "default",
                skip_insert_user_table=True,
            )
        except APIError:
            pass
        ln_setup.login("testuser2@lamin.ai")
        ln_setup.connect(
            "https://lamin.ai/testuser1/static-test-instance-private-sqlite", _test=True
        )
        assert (
            ln_setup.settings.instance.storage.root_as_str
            == "s3://lamindb-setup-private-bucket"
        )
        collaborator_handler.delete_by_slug(
            "testuser1/static-test-instance-private-sqlite", "testuser2"
        )
        # make the instance private
        with pytest.raises(InstanceNotFoundError):
            ln_setup.connect(
                "https://lamin.ai/testuser1/static-test-instance-private-sqlite",
                _test=True,
            )


def test_connect_after_private_public_switch():
    # can't currently test this on staging as I'm missing the accounts
    if os.getenv("LAMIN_ENV") == "prod":
        # this assumes that testuser1 is an admin of static-test-instance-private-sqlite
        ln_setup.login("testuser1@lamin.ai")
        ln_setup.connect(
            "https://lamin.ai/testuser1/static-test-instance-private-sqlite", _test=True
        )
        admin_hub = connect_hub_with_auth()
        # make the instance private
        update_instance(
            instance_id=ln_setup.settings.instance._id,
            instance_fields={"public": False},
            client=admin_hub,
        )
        # attempt to load instance with non-collaborator user
        ln_setup.login("testuser2")
        with pytest.raises(InstanceNotFoundError):
            ln_setup.connect(
                "https://lamin.ai/testuser1/static-test-instance-private-sqlite",
                _test=True,
            )
        # make the instance public
        update_instance(
            instance_id=ln_setup.settings.instance._id,
            instance_fields={"public": True},
            client=admin_hub,
        )
        # load instance with non-collaborator user, should work now
        ln_setup.connect(
            "https://lamin.ai/testuser1/static-test-instance-private-sqlite", _test=True
        )
        # make the instance private again
        update_instance(
            instance_id=ln_setup.settings.instance._id,
            instance_fields={"public": False},
            client=admin_hub,
        )


def test_connect_with_db_parameter():
    # no more db parameter, it is _db now and is hidden in kwargs
    # this also tests that only allowed kwargs can be used
    with pytest.raises(TypeError):
        ln_setup.connect("laminlabs/lamindata", db="some_db")

    if os.getenv("LAMIN_ENV") == "prod":
        # take a write-level access collaborator
        ln_setup.login("testuser1")
        # test load from hub
        ln_setup.connect("laminlabs/lamindata", _test=True)
        assert "root" in ln_setup.settings.instance.db
        # test load from provided db argument
        db = "postgresql://testdbuser:testpwd@database2.cmyfs24wugc3.us-east-1.rds.amazonaws.com:5432/db1"
        ln_setup.connect("laminlabs/lamindata", _db=db, _test=True)
        assert "testdbuser" in ln_setup.settings.instance.db
        # test ignore loading from cache because hub result has >read access
        ln_setup.connect("laminlabs/lamindata", _test=True)
        assert "root" in ln_setup.settings.instance.db

        # now take a user that has no collaborator status
        ln_setup.login("testuser2")
        # the cached high priviledge connection string remains active
        ln_setup.connect("laminlabs/lamindata", _test=True)
        assert "root" in ln_setup.settings.instance.db
        # now pass the connection string
        ln_setup.connect("laminlabs/lamindata", _db=db, _test=True)
        assert "testdbuser" in ln_setup.settings.instance.db
        # now the cache is used
        ln_setup.connect("laminlabs/lamindata", _test=True)
        assert "testdbuser" in ln_setup.settings.instance.db

        # test corrupted input
        db_corrupted = "postgresql://testuser:testpwd@wrongserver:5432/db1"
        with pytest.raises(ValueError) as error:
            ln_setup.connect("laminlabs/lamindata", _db=db_corrupted, _test=True)
        assert error.exconly().startswith(
            "ValueError: The local differs from the hub database information"
        )
