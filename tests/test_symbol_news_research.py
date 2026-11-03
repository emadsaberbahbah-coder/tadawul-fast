"""Execute the reader SQL with a stdlib SQLite adapter, not a mirrored model.

This adapter supplies the small GoogleSQL scalar/window surface used by these
two views. It is an offline behavioral check, not a BigQuery server dry-run.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
import re
import sqlite3
import unittest
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-11-03T12:00:00+00:00"
ITEM_COLS = [
    "symbol", "published_at", "first_seen_at", "captured_at", "snapshot_date", "run_id",
    "title", "original_url", "domain", "publisher_name", "source_feed", "source_kind", "source_item_id",
    "polarity", "sentiment_label", "sentiment_basis", "severity", "tier", "rumour_denial",
    "cluster_grade", "cluster", "topic",
]
KEY_COLS = ITEM_COLS + ["link", "article_key", "display_at", "time_basis", "freshness_state", "link_state"]
RANK_COLS = KEY_COLS + ["archive_copy_count", "last_archived_at", "has_headline_variants", "has_publication_time_variants", "seen_in_eodhd", "seen_in_gate"]


def _body(filename):
    text = (ROOT / "sql" / "bigquery" / filename).read_text()
    return text.split("\nAS\n", 1)[1].strip().rstrip(";")


def _sqlite_sql(sql):
    # Translate syntax only. Conditions, branches, windows and joins are the
    # actual source SQL and run in the database for every behavioral fixture.
    sql = re.sub(r"`\{\{DS\}\}\.([a-z_]+)`", r"\1", sql)
    sql = sql.replace("i.* EXCEPT (title, original_url)",
                      ", ".join("i." + c for c in ITEM_COLS if c not in {"title", "original_url"}))
    # clean's projection changes column order, but downstream wildcard names
    # matter rather than their ordinal positions.
    sql = sql.replace("r.* EXCEPT (article_rank)", ", ".join("r." + c for c in RANK_COLS))
    sql = sql.replace("TO_JSON_STRING(k)", "json_object(" + ", ".join(
        "'" + c + "', k." + c for c in KEY_COLS) + ")")
    sql = sql.replace("TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)", "tfb_cutoff()")
    sql = sql.replace("CURRENT_TIMESTAMP()", "tfb_now()")
    sql = re.sub(r"TIMESTAMP_DIFF\((tfb_now\(\)), ([a-z_.]+), HOUR\)", r"ts_diff(\1, \2, 'HOUR')", sql)
    sql = sql.replace("NET.HOST(", "net_host(")
    sql = sql.replace(" AS STRING)", " AS TEXT)").replace("UNION DISTINCT", "UNION")
    sql = re.sub(r"r'((?:[^']|'')*)'", lambda m: "'" + m[1] + "'", sql)
    return sql


class _CountIf:
    def __init__(self):
        self.n = 0

    def step(self, value):
        self.n += bool(value)

    def finalize(self):
        return self.n


class NewsSql(unittest.TestCase):
    def setUp(self):
        self.now = NOW
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.create_function("tfb_now", 0, lambda: self.now)
        self.db.create_function("tfb_cutoff", 0, lambda: (dt.datetime.fromisoformat(self.now) - dt.timedelta(days=30)).isoformat())
        self.db.create_function("IF", 3, lambda c, a, b: a if c else b)
        self.db.create_function("CONCAT", -1, lambda *xs: None if None in xs else "".join(map(str, xs)))
        self.db.create_function("REGEXP_CONTAINS", 2, lambda s, p: None if s is None else bool(re.search(p, s)))
        self.db.create_function("REGEXP_REPLACE", 3, lambda s, p, r: None if s is None else re.sub(p, r, s))
        self.db.create_function("REGEXP_EXTRACT", 2, lambda s, p: re.search(p, s).group(1) if s and re.search(p, s) else None)
        self.db.create_function("net_host", 1, lambda s: urlsplit(s).hostname if s else None)
        self.db.create_function("ts_diff", 3, lambda end, start, unit: int((dt.datetime.fromisoformat(end) - dt.datetime.fromisoformat(start)).total_seconds() / 3600))
        self.db.create_function("DATE", 2, lambda s, tz: (dt.datetime.fromisoformat(s) + dt.timedelta(hours=3)).date().isoformat() if s else None)
        self.db.create_aggregate("COUNTIF", 1, _CountIf)
        self.db.execute("CREATE TABLE symbol_news (symbol, published_at, captured_at, snapshot_date, run_id, title, link, domain, polarity, tags)")
        self.db.execute("CREATE TABLE v_news_items (symbol, published_at, first_seen_at, captured_at, snapshot_date, run_id, headline, url, domain, source, item_id, direction_label, severity, tier, rumour_denial, cluster_grade, cluster, event_type)")
        self.db.execute("CREATE TABLE v_universe_latest (symbol, symbol_label, name)")
        self.db.executemany("INSERT INTO v_universe_latest VALUES (?, ?, ?)", [
            (s, s + " • Company", s + " Corp") for s in ("AAPL", "MSFT", "NVDA")])
        self.db.execute("CREATE VIEW v_symbol_news_research AS " + _sqlite_sql(_body("74_v_symbol_news_research.sql")))
        self.db.execute("CREATE VIEW v_symbol_news_coverage AS " + _sqlite_sql(_body("75_v_symbol_news_coverage.sql")))

    def tearDown(self):
        self.db.close()

    def provider(self, title="Results", link="https://example.com/story", pub="2026-11-02T10:00:00+00:00", symbol="AAPL", captured=NOW, polarity=0.5):
        self.db.execute("INSERT INTO symbol_news VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (symbol, pub, captured, captured[:10], "run", title, link, "metadata.example", polarity, "earnings"))

    def gate(self, title="Results", link="http://EXAMPLE.com/story#top", pub="2026-11-02T10:00:00+00:00", symbol="AAPL", captured="2026-11-02T20:00:00+00:00", item="item1"):
        self.db.execute("INSERT INTO v_news_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (symbol, pub, "2026-11-02T11:00:00+00:00", captured, captured[:10], "gate-run", title, link, "example.com", "google_news", item, "negative", "H", "A", "denial", "A", "cluster", "earnings"))

    def rows(self, view="v_symbol_news_research"):
        return [dict(r) for r in self.db.execute("SELECT * FROM " + view + " ORDER BY symbol, article_key" if view.endswith("research") else "SELECT * FROM " + view + " ORDER BY symbol")]

    def test_cross_feed_dedup_preserves_provenance_and_capture(self):
        self.provider(captured="2026-11-02T12:00:00+00:00")
        self.provider()
        self.gate()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["feed_count"], r["archive_copy_count"]), (2, 3))
        self.assertEqual((r["source_kind"], r["sentiment_basis"], r["sentiment_label"]), ("gate", "gate_event_direction", "negative"))
        self.assertEqual((r["tier"], r["severity"], r["rumour_denial"]), ("A", "H", "denial"))
        self.assertIsNone(r["publisher_name"])
        self.assertEqual(r["source_feed"], "google_news")
        self.assertEqual(r["link_domain"], "example.com")
        self.assertEqual(r["last_archived_at"], NOW)
        self.assertEqual(r["archive_age_hours"], 0)
        self.assertEqual(r["symbol_label"], "AAPL • Company")
        self.assertEqual(r["published_at"], "2026-11-02T10:00:00+00:00")

    def test_blank_links_do_not_collapse_distinct_titles(self):
        self.provider(title="Acquisition", link=" ")
        self.provider(title="Dividend", link="")
        self.assertEqual(len(self.rows()), 2)
        self.assertTrue(all(r["link_state"] == "missing_link" for r in self.rows()))

    def test_conservative_identity_keeps_query_path_case_and_symbol(self):
        for link in ("https://example.com/story?article=1", "https://example.com/story?article=2", "https://example.com/Story?article=1", "https://example.com:8443/story", "https://example.com:9443/story"):
            self.provider(link=link)
        self.provider(link="https://example.com/story?article=1", symbol="MSFT")
        self.assertEqual(len(self.rows()), 6)

    def test_unsafe_links_never_become_clickable(self):
        for i, link in enumerate(("javascript:alert(1)", "file:///tmp/news", "https://good.example@evil.example/story", "https://example.com/a b", "https://example.com:1:2/story", "https://example.com:65536/story", "https://example.com/\\story", "https://example.com/\x00story")):
            self.provider(title=str(i), link=link)
        for r in self.rows():
            self.assertIsNone(r["link"])
            self.assertEqual(r["link_state"], "unsafe_or_invalid_link")
            self.assertFalse(r["is_recent_linked_article"])

    def test_missing_and_future_publication_not_silently_recent(self):
        self.gate(pub=None)
        self.provider(title="Future", link="https://example.com/future", pub="2026-11-05T10:00:00+00:00")
        rows = {r["title"]: r for r in self.rows()}
        self.assertIsNone(rows["Results"]["published_at"])
        self.assertEqual(rows["Results"]["time_basis"], "first_seen_only")
        self.assertEqual(rows["Results"]["freshness_state"], "unknown_publication_time")
        self.assertEqual(rows["Future"]["freshness_state"], "future_timestamp")
        self.assertIsNone(rows["Future"]["article_age_hours"])
        self.assertFalse(any(r["is_recent_linked_article"] for r in rows.values()))

    def test_updated_url_surfaces_variants_without_claiming_corroboration(self):
        self.provider(title="Initial results", pub="2026-11-01T10:00:00+00:00")
        self.provider(title="Updated results", pub="2026-11-02T10:00:00+00:00")
        r = self.rows()[0]
        self.assertEqual(r["archive_copy_count"], 2)
        self.assertEqual(r["feed_count"], 1)
        self.assertTrue(r["has_headline_variants"])
        self.assertTrue(r["has_publication_time_variants"])
        self.assertEqual(r["title"], "Updated results")

    def test_riyadh_date_and_30day_boundary(self):
        self.provider(title="Boundary", link="https://example.com/boundary", pub="2026-10-04T12:00:00+00:00")
        self.provider(title="Too old", link="https://example.com/old", pub="2026-10-04T11:59:59+00:00")
        self.provider(title="Evening", link="https://example.com/evening", pub="2026-11-02T22:30:00+00:00")
        rows = {r["title"]: r for r in self.rows()}
        self.assertEqual(rows["Boundary"]["freshness_state"], "recent_30d")
        self.assertEqual(rows["Too old"]["freshness_state"], "older_than_30d")
        self.assertEqual(rows["Evening"]["news_date"], "2026-11-03")

    def test_rank_is_stable_for_conflicting_equal_time_copies(self):
        self.provider(title="Tie", polarity=0.5)
        self.provider(title="Tie", polarity=-0.5)
        first = self.rows()[0]
        self.db.execute("DELETE FROM symbol_news")
        self.provider(title="Tie", polarity=-0.5)
        self.provider(title="Tie", polarity=0.5)
        self.assertEqual(first, self.rows()[0])

    def test_coverage_distinguishes_absence_quality_stale_and_refresh_aging(self):
        self.provider()
        self.provider(title="Old", symbol="MSFT", pub="2026-09-01T10:00:00+00:00")
        by_symbol = {r["symbol"]: r for r in self.rows("v_symbol_news_coverage")}
        self.assertEqual(by_symbol["AAPL"]["coverage_state"], "recent_archived_evidence")
        self.assertEqual(by_symbol["MSFT"]["coverage_state"], "no_recent_valid_linked_evidence")
        self.assertEqual(by_symbol["NVDA"]["coverage_state"], "not_observed_in_archive")
        self.assertEqual(by_symbol["NVDA"]["recent_linked_articles_30d"], 0)
        self.assertEqual(by_symbol["NVDA"]["collection_check_state"], "unknown_no_per_symbol_receipt")
        self.assertIsNone(by_symbol["NVDA"]["latest_article_capture_at"])
        self.now = "2026-12-10T12:00:00+00:00"
        by_symbol = {r["symbol"]: r for r in self.rows("v_symbol_news_coverage")}
        self.assertEqual(by_symbol["AAPL"]["coverage_state"], "no_recent_valid_linked_evidence")

    def test_empty_titles_macro_and_future_capture(self):
        self.provider(title=" ")
        self.gate(symbol=None)
        self.provider(title="Visible", captured="2026-11-04T12:00:00+00:00")
        self.assertEqual(len(self.rows()), 1)
        self.assertIsNone(self.rows()[0]["archive_age_hours"])

    def test_reader_does_not_change_rating_or_gate_inputs(self):
        import hashlib
        # Installed query files, independently pinned before this candidate.
        expected = {
            "62_v_symbol_news.sql": "1e48d645856426b096cd9dc04ba594e0146f58b9a79de89bcb3b4e44a41c9bfd",
            "63_v_symbol_rating.sql": "3a07d374b2b1c175e9dd040a0fbd87b6dda158b1d35edf00bf56d263a2b7d0d1",
        }
        for filename, digest in expected.items():
            body = _body(filename).replace("{{DS}}", "project-2e3530e6-2311-4aa3-b36.tfb_archive")
            self.assertEqual(hashlib.sha256(body.encode()).hexdigest(), digest)
        for filename in expected:
            self.assertNotIn("v_symbol_news_research", (ROOT / "sql" / "bigquery" / filename).read_text())

    def test_standalone_custom_queries_match_views_and_execute(self):
        ds = "project-2e3530e6-2311-4aa3-b36.tfb_archive"
        research = _body("74_v_symbol_news_research.sql").replace("{{DS}}", ds)
        coverage = _body("75_v_symbol_news_coverage.sql").replace("{{DS}}", ds)
        coverage = "WITH news_reader AS (\n" + research + "\n),\n" + coverage[len("WITH "):].replace("`" + ds + ".v_symbol_news_research`", "news_reader")
        for filename, expected in (("symbol_news_research_query.sql", research), ("symbol_news_coverage_query.sql", coverage)):
            actual = (ROOT / "docs" / "evidence" / filename).read_text().split("\n", 1)[1].strip().rstrip(";")
            self.assertEqual(actual, expected)
            # Both variants execute against the same fixtures without creating
            # any view in the target cloud dataset.
            actual = actual.replace("`" + ds + ".", "`{{DS}}.")
            result = self.db.execute(_sqlite_sql(actual)).fetchall()
            self.assertEqual(len(result), 0 if filename.startswith("symbol_news_research") else 3)


if __name__ == "__main__":
    unittest.main()
