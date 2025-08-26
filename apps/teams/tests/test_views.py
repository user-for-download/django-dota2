# /home/ubuntu/dota/apps/teams/tests/test_views.py
import pytest
from django.urls import reverse

from apps.teams.models import Team


@pytest.mark.django_db(transaction=True)
async def test_team_detail_view_async(async_client):
    """Tests the async team detail view."""
    team = await Team.objects.acreate(team_id=123, name="Test Team")

    url = reverse("teams:detail", args=[team.team_id])
    response = await async_client.get(url)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["team_id"] == 123
    assert response_data["name"] == "Test Team"
