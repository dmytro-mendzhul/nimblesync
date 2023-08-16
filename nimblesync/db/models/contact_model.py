from typing import Optional
from pydantic import BaseModel


class ContactModel(BaseModel):
    __table__ = "contact"
    """Contant model."""

    id: int
    external_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None

