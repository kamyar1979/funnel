from datetime import datetime
from typing import Optional

from backpack import ActionType, AttachmentType
from sqlalchemy import DateTime, Enum, ForeignKey, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase


class Base(DeclarativeBase):
    pass


class AssistantAction(Base):
    __tablename__ = "assistant_actions"
    action_id: Mapped[str] = mapped_column("action_id", ForeignKey("actions.action_id"), primary_key=True)
    assistant_id: Mapped[str] = mapped_column("assistant_id", ForeignKey("agents.model_id"), primary_key=True)
    attachment_type: Mapped[AttachmentType] = mapped_column(
        Enum(AttachmentType, name="action_attachment_type", create_type=True), nullable=True, primary_key=True
    )
    action: Mapped["Action"] = relationship(back_populates="attached_assistants", lazy="noload")


class Action(Base):
    __tablename__ = "actions"

    # Primary key: generate a UUID via a database function (adjust as needed)
    action_id: Mapped[str] = mapped_column(
        String, primary_key=True, index=True, server_default=text("gen_random_uuid()")
    )

    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    slug: Mapped[str] = mapped_column(String, nullable=False, index=True, unique=True, info={"type": "slug"})
    workspace_id: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[ActionType] = mapped_column(
        Enum(ActionType, name="action_type_enum", create_type=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_by: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), index=True)
    attached_assistants: Mapped[list["AssistantAction"]] = relationship(
        back_populates="action",
        lazy="joined",  # Equivalent to joinedload(Action.attached_assistants)
    )
