from app.subscriptions import hash_link, normalize_link


def test_normalize_link_strips_fragment_tail() -> None:
    raw = "  vmess://abc123#US Fast Node  "
    assert normalize_link(raw) == "vmess://abc123"


def test_hash_uses_normalized_link() -> None:
    base = "vless://example"
    with_comment = "vless://example#comment"

    assert hash_link(base) == hash_link(with_comment)
