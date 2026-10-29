"""Разбор ответов OKX: ctVal инструмента и номинал сделки в USDT."""

from __future__ import annotations

from scanner.parsers import parse_instrument, parse_trade


def test_instrument_contract_value() -> None:
    inst = parse_instrument({"instId": "BTC-USDT-SWAP", "ctVal": "0.01", "ctMult": "1", "settleCcy": "USDT"}, "SWAP")
    assert inst is not None
    assert inst.base == "BTC"
    assert abs(inst.contract_value - 0.01) < 1e-12


def test_trade_notional_in_usdt() -> None:
    trade = parse_trade(
        "BTC-USDT-SWAP",
        {"px": "60000", "sz": "2", "side": "buy", "ts": "1700000000000"},
        ct_val=0.01,
    )
    assert trade is not None
    assert abs(trade.notional - 1200.0) < 1e-9, trade.notional
