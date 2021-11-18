import mock
import pytest
from fastapi import HTTPException

from exodus_gw.models import Task
from exodus_gw.routers import deploy
from exodus_gw.settings import get_environment


@mock.patch("exodus_gw.worker.deploy_config")
def test_deploy_config(mock_deploy, db, fake_config):
    """Ensure deploy_config delegates to worker correctly and creates a task."""

    deploy_task = deploy.deploy_config(
        config=fake_config,
        env=get_environment("test"),
        db=db,
    )

    assert isinstance(deploy_task, Task)

    mock_deploy.assert_has_calls(
        calls=[
            mock.call.send(
                config=fake_config,
                env="test",
                from_date=mock.ANY,
            )
        ],
    )


@mock.patch("exodus_gw.worker.deploy_config")
@pytest.mark.parametrize(
    "data",
    [
        {
            "listing": {
                "/origin/../rhel/server": {
                    "values": ["8"],
                    "var": "releasever",
                }
            }
        },
        {"listing": {"/origin/rhel/server": {"values": ["8"], "var": "nope"}}},
        {
            "rhui_alias": [
                {"dest": "/../rhel/rhui/server", "src": "/../rhel/rhui/server"}
            ]
        },
        {"no_dont": 123},
    ],
    ids=[
        "listing_path",
        "listing_var",
        "alis_path",
        "additional_property",
    ],
)
def test_deploy_config_bad_listing(mock_deploy, db, data, fake_config, caplog):
    """Ensure schema is enforced."""

    # Add bad config data.
    fake_config.update(data)

    with pytest.raises(HTTPException) as exc_info:
        deploy.deploy_config(
            config=fake_config,
            env=get_environment("test"),
            db=db,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Invalid configuration structure"
    assert "Invalid config" in caplog.text

    mock_deploy.assert_not_called()
