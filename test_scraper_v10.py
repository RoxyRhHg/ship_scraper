from scraper_v10 import Config, SessionPool, ShipScraperV10


def test_download_batch_wait_budget_covers_all_retries(tmp_path):
    config = Config(output_dir=tmp_path, request_timeout=30)
    scraper = ShipScraperV10(config)

    assert scraper._download_batch_wait_budget() >= 96


def test_reject_download_failure_tracks_failed_url_and_reason(tmp_path):
    config = Config(output_dir=tmp_path)
    scraper = ShipScraperV10(config)
    url = "https://example.invalid/ship.jpg"

    scraper._reject_download_failure(url, TimeoutError("slow"))

    assert url in scraper.failed_urls
    assert scraper.rejection_counts["download_failed"] == 1
    assert scraper.rejection_counts["download_failed_TimeoutError"] == 1


def test_session_pool_can_disable_environment_proxy():
    pool = SessionPool(proxy_url="", pool_size=1, trust_env=False)
    try:
        assert pool.get().trust_env is False
    finally:
        pool.close_all()


def test_batch_loop_can_stop_at_max_batches(tmp_path):
    config = Config(output_dir=tmp_path, batch_size=10, max_batches=3)
    scraper = ShipScraperV10(config)

    assert scraper.config.max_batches == 3
