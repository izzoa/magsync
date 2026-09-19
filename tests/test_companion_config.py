import pytest
from magsync.config import ConfigurationConflict, load_config, save_config
from magsync.core.models import Subscription


def test_field_merges_preserve_overlapping_subscribe_and_healing(tmp_path, monkeypatch):
    monkeypatch.setenv('MAGSYNC_CONFIG_DIR', str(tmp_path))
    subscriptions, constants = load_config(), load_config()
    subscriptions.subscriptions.append(Subscription(query='Science "News"'))
    constants.limewire.file_iv_b64 = 'new-iv'
    save_config(subscriptions)
    save_config(constants)
    saved = load_config()
    assert saved.subscriptions[0].query == 'Science "News"'
    assert saved.limewire.file_iv_b64 == 'new-iv'


def test_same_field_conflict_and_environment_conflict(tmp_path, monkeypatch):
    monkeypatch.setenv('MAGSYNC_CONFIG_DIR', str(tmp_path))
    a, b = load_config(), load_config()
    a.subscriptions.append(Subscription(query='A'))
    b.subscriptions.append(Subscription(query='B'))
    save_config(a)
    with pytest.raises(ConfigurationConflict):
        save_config(b)
    monkeypatch.setenv('MAGSYNC_SUBSCRIPTIONS','Managed')
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query='ignored'))
    with pytest.raises(ConfigurationConflict):
        save_config(cfg)


def test_readonly_and_unrelated_external_edit(tmp_path, monkeypatch):
    monkeypatch.setenv('MAGSYNC_CONFIG_DIR', str(tmp_path))
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query='A'))
    save_config(cfg)
    stale = load_config()
    path = tmp_path/'config.toml'
    path.write_text(path.read_text()+'\n[extension]\nvalue = "preserved"\n')
    stale.limewire.file_iv_b64 = 'updated'
    save_config(stale)
    assert 'preserved' in path.read_text()
    path.chmod(0o444)
    stale.limewire.file_iv_b64 = 'again'
    with pytest.raises(ConfigurationConflict):
        save_config(stale)
