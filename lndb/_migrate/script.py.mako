"""${message}."""
from alembic import op
import sqlalchemy as sa  # noqa
import sqlmodel as sqm  # noqa
${imports if imports else ""}
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
