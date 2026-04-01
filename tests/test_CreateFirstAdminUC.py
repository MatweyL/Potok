import pytest

from service.domain.use_cases.external.auth.create_first_admin import CreateFirstAdminUCRq


@pytest.mark.asyncio
async def test_create_success(create_first_admin_uc):
    response = await create_first_admin_uc.apply(CreateFirstAdminUCRq(username='admin',
                                                           password='admin'))
    assert response.success
