"""remove description_ru from lots

Revision ID: d7f0124b07b0
Revises: 008ba9a56474
Create Date: 2026-02-27 19:59:33.154500

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7f0124b07b0'
down_revision: Union[str, Sequence[str], None] = '008ba9a56474'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column('lots', 'description_ru')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('lots', sa.Column('description_ru', sa.String(), nullable=True))