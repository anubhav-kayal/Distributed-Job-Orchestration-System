import pytest

from djos.autoscaler import ScalingPolicy


def policy(**kw) -> ScalingPolicy:
    return ScalingPolicy(**{"min_workers": 2, "max_workers": 8, "target_per_worker": 10, **kw})


@pytest.mark.parametrize(
    ("backlog", "expected"),
    [(0, 2), (5, 2), (21, 3), (60, 6), (10_000, 8), (-3, 2)],
)
def test_recommendation_is_backlog_over_target_clamped(backlog, expected):
    assert policy().recommend(backlog) == expected


def test_scales_up_immediately():
    p = policy()
    assert p.desired(current=2, backlog=75, now=0) == 8


def test_scale_down_waits_out_the_stabilization_window():
    p = policy(scale_down_window=30)
    assert p.desired(current=2, backlog=80, now=0) == 8
    # The backlog is gone, but recently we needed 8: hold.
    assert p.desired(current=8, backlog=0, now=10) == 8
    assert p.desired(current=8, backlog=0, now=29) == 8
    # Once the high recommendation has aged out of the window, come down.
    assert p.desired(current=8, backlog=0, now=31) == 2


def test_scale_down_goes_to_the_highest_recent_need_not_the_lowest():
    p = policy(scale_down_window=30)
    p.desired(current=2, backlog=80, now=0)  # needed 8
    p.desired(current=8, backlog=40, now=20)  # needed 4
    assert p.desired(current=8, backlog=0, now=35) == 4  # the 8 aged out; 4 hasn't


def test_spiky_load_does_not_flap():
    p = policy(scale_down_window=30)
    counts = []
    current = 2
    for t in range(0, 120, 5):
        backlog = 80 if t % 20 == 0 else 0  # a burst every 20s, idle in between
        current = p.desired(current, backlog, now=t)
        counts.append(current)
    assert set(counts[1:]) == {8}  # stays up instead of bouncing 8 → 2 → 8


@pytest.mark.parametrize("kw", [{"min_workers": 0}, {"min_workers": 9}, {"target_per_worker": 0}])
def test_invalid_policies_are_rejected(kw):
    with pytest.raises(ValueError):
        policy(**kw)
