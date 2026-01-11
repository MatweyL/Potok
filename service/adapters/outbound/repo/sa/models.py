from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from service.adapters.outbound.repo.sa.base import Base, TablenameMixin, SerialPKMixin, LoadTimestampMixin


class Payload(Base, TablenameMixin, SerialPKMixin, LoadTimestampMixin):
    data: Mapped[dict] = mapped_column(JSON, )
