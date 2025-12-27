from datetime import date

from app.services.ipat_section import compute_section_start


def test_compute_section_start_anchor_and_backtrack():
    # Sale days: 12/20, 12/21, 12/22 are consecutive -> section start is 12/20
    sale_days = {date(2025, 12, 20), date(2025, 12, 21), date(2025, 12, 22)}

    def is_sale_day(d: date) -> bool:
        return d in sale_days

    start = compute_section_start(today=date(2025, 12, 27), is_sale_day=is_sale_day, lookback_days=30)
    assert start == date(2025, 12, 20)


def test_compute_section_start_none_when_no_sale_day_in_lookback():
    def is_sale_day(_: date) -> bool:
        return False

    start = compute_section_start(today=date(2025, 12, 27), is_sale_day=is_sale_day, lookback_days=7)
    assert start is None
