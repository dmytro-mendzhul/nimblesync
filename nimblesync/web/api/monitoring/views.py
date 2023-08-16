from fastapi import APIRouter, Depends
from nimblesync.db.dao.contact_dao import ContactDAO
from nimblesync.db.dependencies import get_contact_dao

from nimblesync.db.models.contact_model import ContactModel
from nimblesync.db.models.list_of_contacts_model import ListOfContactsModel

router = APIRouter()


@router.get("/health")
def health_check() -> None:
    """
    Checks the health of a project.

    It returns 200 if the project is healthy.
    """

@router.get(
            "/contact",
            response_model=ListOfContactsModel,
            name="Search for contacts", tags=['contact'])
async def contacts(
        contactDAO: ContactDAO = Depends(get_contact_dao),
        search: str = "",
        limit: int = 10,
        offset: int = 0,
        includeRemoved: bool = False) -> ListOfContactsModel:
    """Search for contacts

    Args:
        search (str, optional): Seqrch query. Defaults to "".
        limit (int, optional): Items per page. Defaults to 10.
        offset (int, optional): Skip. Defaults to 0.
        includeRemoved (bool, optional): If true, removed contacts will be included. Defaults to False.

    Returns:
        ListOfContactsModel: Result contact items.
    """

    if search:
        contacts = await contactDAO.search_contacts(search, limit, offset, includeRemoved)
    else:
        contacts = await contactDAO.get_all_contacts(limit, offset, includeRemoved)
    
    resp = ListOfContactsModel(contacts=contacts)
    return resp



