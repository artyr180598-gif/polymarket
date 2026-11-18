from scripts.analyze_target_wallet import classify_maker_taker


def _row(tx: str, size: str, price: str, side: str = "BUY") -> dict:
    return {
        "transaction_hash": tx,
        "condition_id": "0x" + "1" * 64,
        "token_id": "123",
        "side": side,
        "size": size,
        "price": price,
        "timestamp": 1795035600,
        "outcome": "Up",
    }


def test_maker_taker_classification_uses_taker_feed_as_subset() -> None:
    taker = _row("0xabc", "5", "0.41")
    maker_a = _row("0xdef", "3", "0.40")
    maker_b = _row("0xghi", "2", "0.39")
    classified, unmatched = classify_maker_taker(
        [taker, maker_a, maker_b],
        [taker],
    )
    assert [row["execution_role"] for row in classified] == ["TAKER", "MAKER", "MAKER"]
    assert unmatched == 0


def test_duplicate_taker_rows_are_subtracted_as_a_multiset() -> None:
    first = _row("0xabc", "5", "0.41")
    second = _row("0xabc", "5", "0.41")
    maker = _row("0xdef", "5", "0.41")
    classified, unmatched = classify_maker_taker(
        [first, second, maker],
        [first, second],
    )
    assert [row["execution_role"] for row in classified] == ["TAKER", "TAKER", "MAKER"]
    assert unmatched == 0
