from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_KEYS = {"spm", "from", "ref", "source", "fbclid", "gclid"}


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return raw

    parts = urlsplit(raw)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")

    if netloc in {"github.com", "www.github.com"}:
        path = path.lower()

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower.startswith("utm_") or key_lower in _TRACKING_KEYS:
            continue
        filtered_query.append((key, value))

    filtered_query.sort(key=lambda pair: (pair[0], pair[1]))
    query = urlencode(filtered_query, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))
