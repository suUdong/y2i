from omx_brainstorm.models import FundamentalSnapshot


def test_fundamental_snapshot_has_analyst_fields():
    snap = FundamentalSnapshot(
        ticker="AAPL",
        recommendation_key="buy",
        recommendation_mean=1.9,
        analyst_count=40,
        target_mean_price=295.0,
        target_median_price=300.0,
        analyst_strong_buy=6,
        analyst_buy=25,
        analyst_hold=15,
        analyst_sell=1,
        analyst_strong_sell=1,
        sector="Technology",
        industry="Consumer Electronics",
    )
    assert snap.recommendation_key == "buy"
    assert snap.recommendation_mean == 1.9
    assert snap.analyst_count == 40
    assert snap.target_mean_price == 295.0
    assert snap.target_median_price == 300.0
    assert snap.analyst_strong_buy == 6
    assert snap.analyst_buy == 25
    assert snap.analyst_hold == 15
    assert snap.analyst_sell == 1
    assert snap.analyst_strong_sell == 1
    assert snap.sector == "Technology"
    assert snap.industry == "Consumer Electronics"


def test_fundamental_snapshot_analyst_defaults():
    snap = FundamentalSnapshot(ticker="TEST")
    assert snap.recommendation_key is None
    assert snap.recommendation_mean is None
    assert snap.analyst_count is None
    assert snap.target_mean_price is None
    assert snap.target_median_price is None
    assert snap.analyst_strong_buy == 0
    assert snap.analyst_buy == 0
    assert snap.analyst_hold == 0
    assert snap.analyst_sell == 0
    assert snap.analyst_strong_sell == 0
    assert snap.sector is None
    assert snap.industry is None
