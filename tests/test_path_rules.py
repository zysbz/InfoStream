from infostream.storage.path_rules import build_item_dir_name, sanitize_windows_component


def test_sanitize_windows_component_replaces_invalid_chars():
    value = 'repo: name* with? bad|chars<>'
    sanitized = sanitize_windows_component(value)
    assert ':' not in sanitized
    assert '*' not in sanitized
    assert '?' not in sanitized
    assert '|' not in sanitized
    assert '<' not in sanitized
    assert '>' not in sanitized


def test_sanitize_windows_component_reserved_name():
    assert sanitize_windows_component('CON') == '_CON'


def test_build_item_dir_name_contains_parts():
    folder = build_item_dir_name('github_search', 'My Repo', 'owner/repo')
    assert folder.startswith('github_search__My_Repo__')