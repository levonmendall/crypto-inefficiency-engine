from inefficiency_engine.history_batch_job import MAX_HISTORY_BATCH_SIZE, select_history_batch


def test_history_batch_has_hard_upper_bound_even_if_misconfigured():
    assets = tuple(f"X{index}" for index in range(40))
    selected = select_history_batch(assets, {"assets": []}, batch_size=40)
    assert len(selected) == MAX_HISTORY_BATCH_SIZE == 8
