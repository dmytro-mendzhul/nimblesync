from pydantic import BaseModel

from nimblesync.db.models.contact_model import ContactModel


class ListOfContactsModel(BaseModel):
    """List of contants response model."""

    contacts: list[ContactModel]

