from datetime import timedelta

import pytest

from infostream.utils.timezone import parse_timezone


def test_parse_timezone_utc_plus_8_alias():
    tz = parse_timezone("UTC+08:00")
    assert tz.utcoffset(None) == timedelta(hours=8)


def test_parse_timezone_asia_shanghai_alias_without_tzdata():
    tz = parse_timezone("Asia/Shanghai")
    assert tz.utcoffset(None) == timedelta(hours=8)


def test_parse_timezone_invalid_raises():
    with pytest.raises(ValueError):
        parse_timezone("NOT_A_TIMEZONE")