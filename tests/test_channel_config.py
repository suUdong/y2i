from omx_brainstorm.app_config import load_app_config


def test_default_channels_include_sampro(tmp_path):
    config = load_app_config(tmp_path / "missing.toml")
    assert any(channel.slug == "sampro" for channel in config.channels)
