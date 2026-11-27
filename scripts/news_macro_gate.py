#!/usr/bin/env python3
# scripts/news_macro_gate.py
"""
================================================================================
TFB News & Macro Gate - v1.13.0 (observe-only; program register A7 / news layer)
================================================================================
v1.13.0 (2026-11-27) -- [SOURCES-REVIEW] YAHOO OFF BY DEFAULT, ARABIC SAUDI SEARCHES, EODHD COMPANY NEWS
WHY (news-sources review the owner set for 27 Nov; owner, 26 Nov 2026 19:13 Riyadh, in Arabic: "if it is ready,
  build it now"). Measured on the runs of 26-27 Nov: yahoo_rss gave 0 headlines inside the window in every run
  (222 of 224 outside on 26 Nov, 221 of 221 on 27 Nov 09:42); Asharq Business refused the runner (HTTP 403) in
  every run, which left the gate with no Arabic feed at all (the Al Jazeera feed is the English one); a Saudi name
  was searched on Google News in English only; and news_now already reads EODHD's company news every hour while
  the gate did not.
WHAT: (1) DEFAULT_SOURCES = ALL_SOURCES without yahoo_rss (OFF_BY_DEFAULT): a blank TFB_NEWS_GATE_SOURCES reads
  every source but Yahoo, and naming yahoo_rss still reads it. (2) Arabic Google News searches on the Saudi
  sites (Argaam, Mubasher, Al Eqtisadiah, Al Arabiya): one MACRO search for the Saudi market (in place of the
  refused Asharq feed), and one per Saudi scope name by its Tadawul code (SAUDI_SITES); the issuer link keeps a
  name item only when the headline gives the code in brackets ('(2222)'), as before for any ticker. Whether
  they return dated, relevant items is read on the first runs on main; the Asharq feed stays until then.
  (3) eodhd_news: EODHD's company news (/api/news, the call news_now makes) for every scope name but the Saudi
  ones, 72-hour window, folded with the other sources by (scope, publisher, headline) and checked by the issuer
  link. EODHD's sentiment fields are NOT read: v_company_news_eval (20 Nov) found the provider's polarity
  positive on 455 of 464 days. (4) Three Arabic feeds on trial in gulf_rss (Argaam's main news and market
  pulse, Mubasher's TDWL news) and a Google News search of sama.gov.sa (SAMA has no feed). No verdict rule, tab
  or column changes.
v1.12.1 (2026-11-27) -- [ISSUER-UNCHECKED] A NAME ITEM WHOSE ISSUER COULD NOT BE CHECKED IS COUNTED AND SAID
WHY: outside engineering audit the owner uploaded on 26 Nov 2026, finding A2; owner, 26 Nov 2026 19:13 Riyadh, in
  Arabic: "if it is ready, build it now". issuer_linked() returns None (cannot judge) for a symbol of the scope with
  no company name and no ticker in the headline, and apply_issuer_link() then kept the item without a word: it went
  on to the clusters, the _News_Items tab and the Exit Plan's news count as if it had been checked.
WHAT: apply_issuer_link() counts those items ('unchecked') and the symbols behind them that have no name ('unnamed',
  a count: the public log names no symbol), and marks each item issuer_checked = False in memory. The run log and
  the digest say "issuer not checked N (no company name for M symbol(s), kept)". Nothing is dropped and no verdict,
  tab, column or count rule changes: the count shows how much of the name news rests on items nobody could tie to
  the company, and names to add to the scope.
v1.12.0 (2026-11-27) -- [AUDIT-GUARDS] FIVE GUARDS FROM THE EXTERNAL ENGINEERING AUDIT OF 26 NOV 2026
WHY (owner, 26 Nov 2026 23:38 Riyadh: an outside review of the whole project; its report, read and checked
  against this code on main 65c7864, findings A1, A3, A4, A6 and E1 reproduced here):
  (A1) an item with no publication date counted as published NOW in the 24 h / 72 h windows of evaluate():
       one undated Google News headline about Iran gave CAUTION on a quiet day. Archive 19-26 Nov: none of
       the 10,144 recorded items was undated or stamped in the future, so no recorded verdict changes.
  (A3) the rumour rule (every member a rumour -> grade 4) applied only below grade 1: three rumours on three
       domains with one wire were grade 1, and NO_NEW_ENTRY on a Gulf anchor. Archive: 44 rumour items, 8
       of them high severity; no recorded verdict rested on them.
  (A4) since 1.10.0 the grade counts origins, but a cluster's direction and the easing majority still
       counted rows: a copied headline voted twice.
  (A6) a market quote was used whatever its age, and an empty input looked like a calm one: no quotes and no
       news gave GO with nothing said.
  (E1) TFB_NEWS_GATE_LOG_DETAIL=on named the holdings on GitHub Actions, and the workflow passed extra symbols
       through a repository Variable, which a public run log prints in its env block.
WHAT: (A1) in_window(): a member counts only with a date, no older than the window and no more than one hour
  ahead of the run. (A3) every member a rumour -> grade 4 unless one member is an official (tier A) source.
  (A4) cluster direction and the easing majority are taken over origins(). (A6) parse_market() keeps the
  quote's own time; evaluate() leaves out a quote older than MARKET_MAX_AGE_H (96 h: a long weekend) and
  names it; the digest gains an "Inputs" line - market quotes used / stale / missing, news sources that
  answered - and says when GO rests on missing inputs. (E1) on GitHub Actions the log never names a holding,
  whatever TFB_NEWS_GATE_LOG_DETAIL says; the workflow reads extra symbols from a secret, not a Variable.
  Verdict thresholds are unchanged. Not changed here (proposed to the owner): the exit plan's use of grade-3
  name news to tighten a stop (audit C1), and an unnamed scope name passing the issuer link (A2).
ROLLBACK: revert this file to v1.11.0.

v1.11.0 (2026-11-26) -- [CENTRAL-BANKS] THREE MORE OFFICIAL FEEDS: BANK OF ENGLAND, BANK OF JAPAN, SWISS NATIONAL BANK
WHY (owner, 26 Nov 2026 18:54 Riyadh, in Arabic: do the world market, the funds, commodities and currencies need
  the same care as the Saudi sources; 19:13: "if it is ready, build it now"): the official group read the Fed,
  the ECB and US agencies and no other central bank. Each feed below was opened on 26 Nov 2026 and held dated
  items of the last two days; each bank publishes it for feed readers and allows personal, non-commercial use.
WHAT: FEEDS gains, in the official_rss group (72 h window, tier A):
       boe   https://www.bankofengland.co.uk/rss/news          RSS 2.0, 50 items
       boj   https://www.boj.or.jp/en/rss/whatsnew.xml          RSS 2.0, 46 items
       snb   https://www.snb.ch/public/rss/en/pressrel          RSS 2.0, 20 items, the date in dc:date (read since 1.5.0)
  Left out on purpose: the People's Bank of China (its terms for automated reading were not found), HKEX
  (its terms forbid reading by a program without written permission), SAMA, OPEC and the IEA (no official feed).
  domain_of() knows the Japanese second levels "or.jp" and "go.jp": a link to www.boj.or.jp was read as the
  domain "or.jp", an unknown source (tier F), wherever Google News or GDELT carried a Bank of Japan page.
  A feed that refuses the runner is named in the run's failure note, as every feed is.
ROLLBACK: revert this file to v1.10.0.

v1.10.0 (2026-11-26) -- [ORIGINS] A HEADLINE COPIED BY SEVERAL SITES IS ONE CONFIRMATION, NOT SEVERAL
WHY (owner, 26 Nov 2026 19:10 Riyadh, in Arabic: what matters is how an item is let through, used, classified,
  "and the measure of whether the item is repeated on other sites"; 19:13: "if it is ready, build it now"):
  the information grade and the "three domains" rules counted DOMAINS. One agency story reprinted under the
  same headline by a network of sites counted as that many confirmations - the archive of 24-26 Nov holds one
  headline on 15 sites of one syndication network, every one of them an unknown source. Archive 19-26 Nov
  (v_news_items): 639 headlines appeared word for word on two or more domains (1,838 of 10,121 items).
WHAT: origins() - members whose headline is the same once a trailing site name is removed (25 characters or
  more after normalising, so a short common headline is not merged) are ONE origin, and the copy from the
  best tier speaks for it. A tail is a site name only when it names the item's own domain or a known
  publisher (is_site_tail); any other short closing clause stays part of the headline. information_grade() grades the origins, and the domain counts of evaluate()'s
  rules count the origins' domains. A copy never raises anything: a site that only says it quotes a wire is
  not given the wire's tier - it merges with the wire's own item when that item is in the cluster, and stays
  its own source when it is not. The driver line says "N sources (M independent)" when the two differ. The
  rows recorded in the tabs do not change, and nothing is dropped from them.
  Archive replay (the 67 recorded runs since 19 Nov, every market-anchored cluster re-graded both ways over
  each run's own 24 hours): in all 67 runs some cluster loses domains once copies count once; no cluster's
  grade or level changes and no recorded verdict changes - the days were confirmed many times over. The
  rule matters on a thin day, when one reprinted story is all there is.
ROLLBACK: revert this file to v1.9.0.

v1.9.0 (2026-11-26) -- [REVIEW-26NOV] FOUR CORRECTIONS FROM THE OWNER'S REVIEW REQUEST OF THE DAY'S WORK
WHY (owner, 26 Nov 2026 16:30 Riyadh: "Please review and try to correct and improve all"):
  (1) RULE GAP. A Gulf high-severity story raised CAUTION when unconfirmed (grade 3-4) and NO_NEW_ENTRY when
      graded 1 from three domains - but nothing when it was confirmed by one or two domains (grade 1-2): a
      blog alone gave CAUTION, the blog plus Reuters gave GO, an official source alone gave GO. More
      confirmation must never lower the verdict. Archive replay (v_news_items / v_news_gate, the 67 recorded
      runs since 19 Nov): 13 runs held such a cluster; every one was NO_NEW_ENTRY already on other
      clusters, so no recorded verdict changes - the gap opens on a quiet day, which the archive has not had.
  (2) NAMES STARVED BY THE ROW CAP. A run records at most TFB_NEWS_GATE_MAX_ITEMS (200) new rows, most severe
      first; 29 of the 67 runs filled the cap, and a medium or low item about a name waited behind every
      market-wide item of the same severity. Archive since 19 Nov: a market-wide item was recorded a median
      2 h after publication, an item about a name 7-17 h after (41 of 102 more than a day after). The
      Exit Plan and Holding Read pages read names from _News_Items, so they saw that news late.
  (3) BLS REFUSED. bls.gov answered HTTP 403 in every run; its terms reserve the right to block robots that
      give no contact for their owner.
  (4) THE MEMORY WAS LOST TO A BUSY WORKBOOK. In 3 of the last 15 runs (2 of the 4 chained runs of 26 Nov)
      the read of _News_Items was refused with HTTP 429 - the workbook's per-minute read quota, right after
      the hourly Exit Plan - and the run went on with no memory: "carried from earlier runs 0" instead of
      about 1,300 items, so a situation confirmed over the day rested on that one run's collection. Only the
      log said so.
WHAT: (1) evaluate(): a Gulf-anchored high-severity story graded 1-2 that does not reach the three domains
  is CAUTION, with the driver "macro (few sources)". (2) pick_new(): within the cap the items about names
  come first, then severity and grade as before; the digest says how many classified items wait for a later
  run and how many of them are about names. No item is dropped - a waiting item is recorded by a later run
  while its source still lists it. (3) feeds(): bls.gov is asked with the declared contact
  (TFB_SEC_USER_AGENT) like sec.gov, and not at all without it. (4) run(): a memory read refused with HTTP
  429 is asked again after 20 s and after 40 s more (the quota is per minute); any other failure is not
  retried; and when the memory could not be read the digest says so in a line of its own.
ROLLBACK: revert this file to v1.8.1.

v1.8.1 (2026-11-26) -- [PUBLIC-LOG] THE PUBLIC RUN LOG NO LONGER NAMES THE HOLDINGS
WHY (the owner's standing rule: "Repo logs are public: no amounts, quantities, account numbers or holdings'
  symbols in run logs"): since v1.0.0 the log printed the scope (holdings first) and the digest listed the
  flagged names and every name cluster with its headline; failure notes since v1.5.0 name the symbol asked.
  The repository is public, so its Actions logs, step summaries and artifacts are too.
WHAT: log_detail() - TFB_NEWS_GATE_LOG_DETAIL = on | off | auto (default auto: off on GitHub Actions, on
  elsewhere), the rule exit_plan 1.6.1 uses. When off: the scope line gives the count only; the digest (log,
  step summary, evidence .md) keeps the verdict, the drivers, the market-wide clusters, the counts and the
  source health, and gives the flagged names and the name clusters as counts; the evidence .json is the same
  public cut (the issuer-link examples, which are name headlines, as counts); and every line printed passes
  mask_public(), which replaces each scope symbol, its letter ticker and its company name with "[name]" (a failure
  note "<symbol>: HTTP 401" reads "[name]: HTTP 401"). The workbook tabs keep every detail - nothing written
  there changes.
ROLLBACK: TFB_NEWS_GATE_LOG_DETAIL=on (repository Variable), or revert this file to v1.8.0.

v1.8.0 (2026-11-26) -- [LLM-SHADOW] A LANGUAGE MODEL READS THE SAME HEADLINES BESIDE THE WORD LISTS
WHY (owner, 26 Nov 2026 15:28 and 15:34: can another AI he subscribes to help the news reading - "let us
  start with this too"): the word lists classified 56% of the headlines collected since 19 Nov and cannot read
  a sentence (v1.4.0 left "Iran attacks US bases ... despite diplomatic efforts" unread as fighting).
WHAT: after the verdict is computed and recorded, llm_shadow() hands the run's fresh headlines to
  scripts/news_llm_reader.py 1.0.0 (Gemini API) and appends its labels beside the rule's own reading to a new
  tab, _News_LLM (15 columns; the reader's header). The reader sees headlines and their kind only - no symbol.
  IT CONTROLS NOTHING: it runs after evaluate() and after the gate's own rows are written; its labels go to
  their own tab and are read by nothing; any failure in it is caught, named in the digest and leaves the
  verdict, the exit code and the three gate tabs exactly as they were (pinned by a test).
  Gate TFB_NEWS_LLM = on (default) | off; without GEMINI_API_KEY nothing is asked. A dry run asks and records
  nothing in the workbook.
NOT IN THIS VERSION: no verdict rule, taxonomy or existing tab changes. The BigQuery copy of _News_LLM and the
  view that scores the two readings against what followed come once the tab holds rows.
ROLLBACK: TFB_NEWS_LLM=off (repository Variable), or revert this file to v1.7.0.

v1.7.0 (2026-11-26) -- [MARKETAUX] COMPANY NEWS FROM MARKETAUX FOR THE FIRST NAMES OF THE SCOPE
WHY (owner, 26 Nov 2026 15:21: the repository secret MARKETAUX_API_KEY added after the review of five news
  services he sent): of the five, Marketaux alone serves its free plan without delay (100 requests a day,
  3 articles a request, 80+ markets, articles matched to the entity - a company - they are about). Its terms
  grant personal, non-commercial use, which is this project's use.
WHAT: source `marketaux` (in NEW_SOURCES: folded, carried, issuer-linked): /v1/news/all once per name for the
  first TFB_NEWS_GATE_MARKETAUX_MAX (default 8, 0-30) scope names it can ask - the scope lists holdings first -
  with filter_entities, English, published in the last 72 h. 8 requests x 9 runs a day stay under the plan's 100;
  past it the service answers HTTP 402 and the source is named and stopped for the run. Marketaux takes the
  key only as the api_token parameter, so - as for EODHD - it is scrubbed from every message. An item is
  filed under the publisher of its URL (else its 'source' domain).
  Without MARKETAUX_API_KEY the source is skipped and named. Its sentiment scores are not read: the gate
  classifies every headline itself.
NOT IN THIS VERSION: verdict rules, taxonomy, tabs and headers are unchanged; observe-only as before.
ROLLBACK: leave marketaux out of TFB_NEWS_GATE_SOURCES, or revert this file to v1.6.0.

v1.6.0 (2026-11-26) -- [FINNHUB] THE FINNHUB KEY THE PROJECT ALREADY HOLDS NOW READS NEWS
WHY (owner, 26 Nov 2026 14:49: "I think it already exists but may be unused"): FINNHUB_API_KEY is set on
  Render, where core/providers/finnhub_provider.py uses it as the last price fallback (quotes only). No
  workflow passes it and no script reads Finnhub's news. Its free plan (personal use; 60 calls a minute,
  30 a second) includes company news for North American issuers and the general market-news feed - an
  aggregator that files each story with its own time, like the publishers' feeds of v1.5.0.
WHAT: source `finnhub` (in NEW_SOURCES: folded, carried, issuer-linked like bing_news / yahoo_rss):
  /news?category=general (market-wide, 24 h) and /company-news per US scope name (from 3 days back, 72 h).
  The key travels in the X-Finnhub-Token header, never in a URL, and is scrubbed from every message. Each
  request is asked once (12 s), 1.1 s apart; two failures in a row stop the source. An item is filed under
  the publisher of its URL; an item whose URL is Finnhub's own redirect is filed under the publisher its
  'source' names when that name is known (FINNHUB_SOURCE_DOMAINS), else under finnhub.io (tier D).
  Without FINNHUB_API_KEY the source is skipped and named under Sources Failed ("FINNHUB_API_KEY not set").
NOT IN THIS VERSION: verdict rules, taxonomy, tabs and headers are unchanged; observe-only as before.
ROLLBACK: leave finnhub out of TFB_NEWS_GATE_SOURCES, or revert this file to v1.5.0.

v1.5.0 (2026-11-26) -- [SOURCES] FIVE MORE SOURCES; A FEED THAT CANNOT BE READ IS NAMED, NOT COUNTED AS ZERO
WHY (owner, 26 Nov 2026 12:42: add three to five news sources and make the gate stronger and faster): the
  archive of the gate's 137 runs (27 Oct - 26 Nov) shows it standing on one source. Google News carried
  about 1,030 of the items of an average run; GDELT was refused (HTTP 429) in every run and returned nothing
  in 67; the Fed feed answered in 134 runs and gave 0 items in all 134 - also in the 7 runs of 12 - 15 Nov,
  days after the Fed's press release of 12 Nov - and no run named a fault. parse_rss() returned [] for any
  reply it could not parse, so a blocked or mis-decoded feed looked the same as a quiet one. (One way this
  happens: requests decodes a text/xml reply that names no charset as ISO-8859-1; a UTF-8 byte-order mark
  then reaches the XML parser as three stray characters and the whole feed is refused.)
WHAT: (1) five keyless sources, each optional and fail-open like the six before them:
       bing_news    Bing News RSS, newest first: the seven English macro searches + each scope name (a
                    second aggregator, so the gate is not blind when Google throttles the runner)
       yahoo_rss    Yahoo Finance headline feed per scope name (US ticker, or the .SR code)
       press_rss    publishers' own feeds: MarketWatch, WSJ Markets, CNBC, BBC Business, NYT Business,
                    Investing.com, Seeking Alpha
       official_rss primary sources: SEC press releases, BEA, BLS, ECB, EIA, White House presidential actions
       gulf_rss     Asharq Business (Arabic), Arab News, The National (business), Al Jazeera
     FEEDS is the registry (feed id, group, URL, publisher domain, language, window in hours). A feed item
     takes its publisher's tier from SOURCE_TIERS as every headline does. An item with no readable date, or
     outside its window (24 h market-wide, 72 h for a name or an official release), is left out: evaluate()
     reads an undated headline as published now. A feed is asked once per run (12 s at most, no retry).
  (2) parse_feed() reads RSS 2.0, RSS 1.0 and Atom from the reply's bytes (byte-order mark and leading blank
     lines removed) and raises on a reply that is not a feed. google_news and fed_rss use it, so such a reply
     is listed under Sources Failed with its first characters (for Google News the next search is still
     asked: the reply does not count toward the two-failure breaker). parse_rss() keeps its old contract ([]).
  (3) fold_new_sources(): a headline from a new source is left out when an older source, or an earlier new
     one, already carries the same headline from the same publisher in the same scope - one story from one
     publisher stays one member of its cluster. No item of the six older sources is ever left out by it.
  (4) the issuer-link rule (v1.1.0) also judges bing_news and yahoo_rss name items; the new news sources are
     carried in the run-to-run memory like gdelt and google_news (official feeds are re-read whole).
  (5) the digest and the evidence file gain one line per source: items kept, left out (outside the window,
     undated, repeated) and the age in minutes of its newest and median item at the run.
THIS CAN CHANGE A VERDICT OR A NAME FLAG. Up: more publishers reach the three-domain rules sooner, and an
  official release grades its cluster 1. Down, in two corners that exist today: a denial from a new source can
  make a cluster "denied", and a Gulf story that one or two publishers confirm (grade 1-2, fewer than three
  domains) raises nothing while the same story unconfirmed (grade 3-4) raises CAUTION - a gap in evaluate()
  since v1.0.0, left as it is here. The gate still controls nothing (observe-only); the Version column of
  _News_Gate marks the runs, so the score can be read before and after.
NOT IN THIS VERSION: the verdict rules, the taxonomy, the 200-row limit per run and GDELT's throttling are
  unchanged. No tab or header changes. The feeds were checked through a web fetch on 26 Nov; which of them
  answer the GitHub runner is known only from the first run - a feed that refuses is named in Sources Failed.
ROLLBACK: TFB_NEWS_GATE_SOURCES=edgar,gdelt,google_news,fed_rss,econ,market (repository Variable) turns the
  five off; or revert this file to v1.4.1.

v1.4.1 (2026-11-21) -- [FOMC-2027] THE FED'S REVISED 2027 SCHEDULE
WHY: the Fed revised its tentative 2027 calendar on 12 Nov 2026 (federalreserve.gov, press release of that
day): the 26-27 Oct 2027 meeting is for strategy and announces no decision, and the 7-8 Dec meeting moved to
16-17 Nov. FOMC_DECISION_DAYS (read 28 Oct 2026) still held 27 Oct and 8 Dec 2027, so from October 2027 the
gate would have named a rate decision on two days without one and missed 17 Nov. The list now ends 15 Sep,
17 Nov 2027; nothing before 2027 changes. tests/test_bq_views.py checks it against v_event_calendar's FOMC days
(views 1.21.0), which were written from the same pages.
v1.4.0 (2026-11-19) -- [TRUCE-READ] FIGHTING BESIDE A TRADE TRUCE IS FIGHTING; A CEASEFIRE THAT IS BREAKING IS NOT EASING
WHY (owner, 19 Nov 2026 16:09: "for what you are waiting for my decision go and select the best option"; the two
  readings v1.3.0 left open):
  (1) the older truce rule turns a fighting headline into de-escalation when it also names a truce - and
      'truce' inside "trade truce" counted, so "Missile strike reported as US-China trade truce holds" was not
      read as fighting;
  (2) every de_escalation reading stood on the EASING line, also when the headline says the ceasefire is
      breaking. The gate's archive (27 Oct - 19 Nov, 352 market-wide de-escalation headlines): 29 carry an
      English breach word and every one was recorded as easing (direction +1), 6 of them in a grade-1 cluster
      - "Trump's Board of Peace Warns Gaza Ceasefire Could Collapse" (Bloomberg), "Iran attacks US bases in
      Gulf countries despite diplomatic efforts" (Reuters), "Israeli army launches military offensive in
      central Gaza despite ceasefire".
WHAT: (1) Classifier.classify(): the older truce rule looks for a ceasefire word with the trade-truce phrases set
  aside, so fighting beside a trade truce stays war_escalation. THIS CAN RAISE A VERDICT - it is the reading the
  rule was written for. (2) A market-wide de_escalation headline that carries a breach word is read as falling
  (direction -1; a name's own headline is left as it was, so no name gains a flag; a denied breach - "no
  ceasefire violations" - does not count). evaluate(): the EASING line
  lists a de-escalation cluster only for its members that are not falling - they must outnumber the falling
  ones and be confirmed (grade 1-2) on their own - and quotes one of their headlines. With no falling member the
  line is exactly as before. This part can only remove an easing line; a de_escalation cluster still raises
  nothing.
NOT IN THIS VERSION: the fighting words themselves are unchanged. "Iran attacks US bases" carries none of them
  ('attack' alone is not listed: it is also "attacks the Fed chair"), so it is no longer easing but is not yet
  read as fighting.
ROLLBACK: revert this file to v1.3.0.

v1.3.0 (2026-11-19) -- [TRADE-RELIEF] A TRADE TRUCE IS NOT A CEASEFIRE; A TARIFF CUT IS NOT TRADE PRESSURE
WHY (owner, 19 Nov 2026 15:33: "Lest move to the next", part 1 of the next stage; the two misreadings v1.2.0
  left on purpose): 'truce' is a ceasefire word, so "China extends US trade truce by two months" was put on the
  EASING line as war de-escalation; 'tariff' is a trade-pressure word, so "China and Canada agree on tariff
  relief" was read as trade_sanctions, a type that raises CAUTION on a market anchor. The gate's own archive
  (27 Oct - 19 Nov, before it searched trade) holds one headline of each kind. In two weeks of world headlines
  (GDELT, 5 - 18 Nov) 106 of 790 'tariff' headlines (13%) speak of relief and 13 of 58 'truce' headlines (22%)
  are trade truces - none of the 13 names a ceasefire or peace talks. v1.2.0 began searching trade, so these
  now arrive daily.
WHAT: taxonomy 1.2.0 adds three word lists (trade_truce, trade_relief, trade_hedge). In Classifier.classify():
  (A) a de_escalation reading whose only ceasefire word sits inside a trade-truce phrase becomes the label-only
      type trade_deal (shown as falling when the headline says the truce is in doubt);
  (B) a trade_sanctions reading becomes trade_deal when the headline states tariff relief (a listed phrase, or a
      relief verb up to two words before 'tariffs'), names no other trade-pressure word once that phrase is set
      aside (the words between the verb and 'tariffs' stay and still count), and carries no hedge (threat,
      demand, plan, talks, if, no, would, end, ruled out, a question mark ...), no breach, denial or rumour
      word, no relief verb in front of the relief phrase ("scraps tariff relief") and no past form of one
      after it ("tariff pause lifted").
  For a name's own headline the reading is dropped instead (a name never takes a label-only type).
  SAFETY, by construction: a reading can only move from de_escalation or trade_sanctions to trade_deal (or to
  none, for a name). trade_deal is in no verdict rule, so the change can remove an easing line, a CAUTION driver
  or a WATCH flag and can never add one. evaluate() is unchanged. Without the three lists (taxonomy 1.1.0) the
  classifier reads exactly as v1.2.0.
NOT IN THIS VERSION: (1) sanctions relief ("lifts sanctions on ...") stays trade_sanctions: the archive holds no
  such headline. (2) A fighting headline that also names a trade truce is still not read as fighting (the older
  truce rule turns it into de-escalation, now trade_deal); closing that can RAISE a verdict, so it waits for the
  owner's word. (3) A hedged relief headline ("seeks lower tariffs") keeps its older, cautious reading.
ROLLBACK: revert this file and data/news_gate_taxonomy.json to v1.2.0 / 1.1.0.

v1.2.0 (2026-11-19) -- [TOPICS] TRADE DEALS, SUMMITS, AI AND ELECTIONS ARE READ AND NAMED
WHY (owner, 19 Nov 2026: is the China summit in the news analysis; world news - politics, wars, the economy, the
  fear of AI, elections - is part of valuing any stock; "Let's Start build the new news"): the gate searched five
  subjects (fighting, oil, rates, market stress, Saudi) and had no word for a summit, a trade deal, AI or an
  election. Archive, 12 - 19 Nov: about 3,000 headlines, 23 on trade; the week of the Shenzhen APEC summit the
  one headline on what was at stake ("A trade truce deadline, new export controls, and Iran") was filed as war
  de-escalation because 'truce' is a ceasefire word, and "FOMC minutes and APEC Summit in focus" as a central-bank
  story.
WHAT: (1) taxonomy 1.1.0 (data/news_gate_taxonomy.json), four MACRO event types: trade_deal (positive),
  summit_diplomacy, ai_tech and election_politics (neutral: the direction comes from the headline's own
  positive / negative words), English and Arabic terms. They are LABELS ONLY ("label_only": true) and can change
  no older reading, by construction: (a) they stand after every older MACRO type and the first match wins, so a
  headline that had a type keeps it (a recession, a default, a tariff, a crash or a strike named beside a summit,
  AI or an election is still read as before - Codex P2, #1031); (b) a name's own headlines never take a
  label-only type, so no new WATCH can appear on a name; (c) no verdict rule names them: evaluate() is unchanged.
  The verdict, its drivers, the easing line and the name flags are the same for any input. (2) Three more
  searches a language: GDELT and Google-News English 'trade china', 'ai tech', 'politics'; Google-News Arabic
  'trade china ar', 'ai tech ar' - after the existing macro searches and before the names.
NOT IN THIS VERSION: (1) two older misreadings stay as they were, because correcting them changes the easing
  line and a CAUTION rule and needs its own evidence: a "trade truce" is read as de_escalation ('truce'), and a
  tariff cut as trade_sanctions ('tariff'). (2) The GDELT document API still answers HTTP 429 to the runner on
  most runs (its name and macro searches are skipped); the daily themes now reach BigQuery by another road
  (scripts/archive_news_themes.py reads the public GDELT tables), and moving the headlines the same way is the
  next step.
ROLLBACK: revert this file and data/news_gate_taxonomy.json to v1.1.0 / 1.0.0. No tab or header change.

v1.1.0 (2026-11-16) -- [ISSUER-LINK] A HEADLINE COUNTS FOR A NAME ONLY IF IT IS ABOUT THAT ISSUER
WHY (owner reviews of 15 and 16 Nov: "news under the wrong company (MBIN, IFS)"; news design phase 0, issuer
  linking): a name's GDELT / Google-News query is its cleaned company name, and every article the search
  returned was filed under that symbol. v_symbol_news, 25 Oct - 16 Nov: MBIN.US (Merchants Bancorp) held
  "Farmers & Merchants Bancorp" results (another bank, FMCB), "TransAct Technologies (TACT) Reports Q3 Loss" and
  ten peer recaps ("Q3 rundown: Axos Financial (NYSE:AX) vs other regional banks stocks"); IFS.US (Intercorp
  Financial Services) held "Inter & Co, Inc. Q3 2026 Earnings Call Summary" and "Inflection Point Q3 2026
  Earnings"; BARC.L and BNP.PA held 60 analyst notes in which the bank is the broker ("KMB Maintained by
  Barclays -- Price Target Raised"). Those items enter the clusters, so a wrong company's results or a broker
  note could flag a name (Names Flagged) and colour its sentiment.
WHAT: issuer_linked(title, symbol, name) (pure) keeps a GDELT / Google-News item for a name only when the
  headline names the issuer: (a) the ticker in a ticker form - '(MBIN)', '$MBIN', 'NASDAQ:MBIN', 'NYSE: IFS',
  'LSE:BBOX' (exchange prefixes in _TICKER_PREFIXES), the number of a .SR name in brackets; or (b) the cleaned
  company name as a whole phrase (case-insensitive) that is not the tail of another company's name ('Farmers &
  Merchants Bancorp') and not the broker of an analyst note ('... by Barclays', 'at BNP Paribas', 'Barclays
  maintains / raises / lowers / adjusts / upgrades / downgrades / initiates / reiterates / cuts / sees' - only
  when the headline carries analyst-note words: price target, rating, upgrade, overweight, coverage ...). A
  ticker form always links. EDGAR filings (matched by CIK), the Fed feed, macro items and a name without a
  known company name are kept as before. Fresh items and items carried from memory both pass the check.
  Script Property-free; env TFB_NEWS_ISSUER_LINK: on (default) drops unlinked items; observe keeps them and only
  counts them; off = v1.0.2. The run report and the digest line count them ('unlinked N dropped' / 'would
  drop'), with up to 5 examples in the evidence file.
ROLLBACK: TFB_NEWS_ISSUER_LINK=off, or revert this file to v1.0.2.

v1.0.2 (2026-10-28) -- A STABLE VERDICT: 24 h MEMORY + ENGLISH WIRES
WHY (the first two runs on main 2f8ead9f, 27 Oct 23:03 and 23:11 UTC, eight
  minutes apart): the dry run read NO_NEW_ENTRY - a Hormuz cluster from 5
  domains with a licensed wire ("oil closes higher as attacks continue in
  the Strait of Hormuz") - and the write run read CAUTION: Google-News
  returned a different set and GDELT throttled the runner (circuit open at
  its second query, 14 queries skipped), so the wire story was not in the
  second fetch. A verdict that flips with feed variance cannot be scored.
  US names were reached only through GDELT, so most were skipped.
WHAT: (1) Memory: before clustering, the run reads back the newest
  _News_Items rows (TFB_NEWS_GATE_MEMORY_ROWS, default 2500; 0 = off) and
  carries every GDELT / Google-News headline published within 24 h (macro)
  or 72 h (a name) into the clusters, re-classified with the current
  taxonomy. A fresh copy wins on the same Item ID, nothing carried is
  written again, EDGAR and the Fed feed are re-read whole as before, and
  the gate row's Items cell shows "+N carried". Dry runs read it too
  (read-only). Only sources selected for the run are carried, so a run
  limited by TFB_NEWS_GATE_SOURCES is not moved by an excluded source
  (Codex review on #862), and a name's rows are carried only while the
  name is in today's scope. (2) Google-News in English (US edition): four macro queries
  (geopolitics, oil, rates, markets) and one per non-Saudi name, beside
  GDELT. (3) The Names Flagged cell lists INFO flags too (earnings
  released), BLOCK first, then WATCH, then INFO.
ROLLBACK: revert this file to v1.0.1 (2f8ead9f). No tab or header change.
--------------------------------------------------------------------------------
v1.0.1 (2026-10-28) -- --write NEEDS A WORKBOOK SESSION
WHY (Codex re-review on PR #859; the fix was pushed after the owner merged
  the PR at 45748da0, so it lands here): with DEFAULT_SPREADSHEET_ID set
  but GOOGLE_SHEETS_CREDENTIALS missing or unreadable, no Sheets session was
  opened and run() skipped its write block - a scheduled --write ran as a
  silent dry run (no scope, no tabs written) and still exited 0.
WHAT: main() returns 2 before collecting anything when --write has no
  usable Sheets session, and says so; a session that fails to open is
  reported (exception type only).
  Second fix (first live dry run 40595558188, 27 Oct 22:32 UTC, still
  collecting after 10 minutes): a throttled or unreachable provider cost
  about a minute per query (two 25 s attempts plus the back-off), with no
  bound across ~20 queries, so the 20-minute job limit was within reach.
  Each multi-query source (gdelt, google_news, edgar) now stops after 2
  consecutive failed queries (circuit open) or once its time budget is spent
  (SOURCE_BUDGET_S: gdelt 240 s, google_news 120 s, edgar 150 s); skipped
  queries are counted in its failure note. The per-request timeout is 20 s
  (TFB_NEWS_GATE_TIMEOUT_S, 5..60). A GDELT plain-text answer (malformed
  query) is an answer, not a breaker failure. One progress line per source
  ("[news_gate v1.0.1] gdelt: N items in Ts | failures") shows in the log.
  Third fix, from the same live run (verdict NO_NEW_ENTRY on 28 Oct 01:32
  Riyadh): its Iran cluster led with "oil falls as diplomatic efforts to
  contain the Iran war grow" - the bare Arabic word for war (harb) is a
  topic, not an escalation. Taxonomy: harb removed from war_escalation;
  war outbreak / declaration, attacks and a burning or hit tanker (EN + AR)
  added; ending the war, diplomatic efforts and containment (EN + AR) added
  to de_escalation. EODHD's "FOMC Statement" no longer doubles the FOMC
  driver (FOMC_NAME_RE).
  Fourth fix (Codex review on PR #861): an attack headline that also names
  diplomacy was read as easing unless a breach marker matched, and failure
  was not one - "Iran launches missile strikes as diplomatic efforts fail"
  read as de-escalation, and from three wires the verdict fell from
  NO_NEW_ENTRY to GO. Breach markers now cover failed, stalled or rejected
  diplomacy, a breakdown, deadlock or impasse and no progress (EN + AR).
  A leading space in a taxonomy term marks a word start (" stall" does not
  match "installed"), and "broke " no longer matches "US-brokered". When
  attack and diplomacy words meet, failure words keep it escalation: the
  safer error for an entry gate.
ROLLBACK: revert this file to v1.0.0 (78faeb6e).
--------------------------------------------------------------------------------
v1.0.0 (2026-10-28)
WHY (owner request 28 Oct 2026, 00:01-00:15 Riyadh): every GO/NO-GO since the
first ticket (26 Oct) has needed a hand-made macro and news check (war news,
energy prices, the Fed, each name's own news), and the existing news archive
(news_archive_collector: Google-News RSS + a lexicon score for a rotating 60
names) never reaches the decision. The FRST post-mortem showed the cost: a
22% EPS miss filed with the SEC two sessions before the entry was not seen
by any rule. The owner asked for the check to run by itself, to grade each
source before believing it, to analyse the economic and financial meaning,
and to measure afterwards whether each call and each source was right -
using free sources (decision 28 Oct 00:11).

WHAT: one run =
  1. SCOPE - holdings (My_Portfolio) + the latest _Shadow_Rank_Log day
     (champion, challenger and board seats), plus TFB_NEWS_GATE_SYMBOLS.
  2. COLLECT (free or already paid; each source optional and fail-open):
       edgar      SEC EDGAR submissions per US symbol (8-K / 6-K items; A)
       gdelt      GDELT DOC 2.0 article lists: macro queries + scope names
       google_news Google-News RSS, Arabic macro queries + Saudi names
       fed_rss    Federal Reserve press releases (A)
       econ       EODHD economic events, US + SA (estimate / actual)
       market     EODHD real-time S&P 500 and VIX
  3. CLASSIFY every headline with the bilingual taxonomy
     (data/news_gate_taxonomy.json: 23 event types, rumour and denial
     markers, positive / negative words, anchors) and GRADE it: source tier
     A (official / issuer wire) .. F (unknown) from SOURCE_TIERS, and an
     Admiralty-style information grade 1 (confirmed) .. 6 (social only) per
     cluster of copies of one story.
  4. GATE - a deterministic verdict GO / CAUTION / NO_NEW_ENTRY with the
     drivers named (EVALUATE below), plus per-name flags BLOCK / WATCH /
     INFO (8-K items, negative confirmed news, an earnings release).
  5. RECORD (with --write): _News_Gate (one row per run), _News_Items
     (new classified headlines, deduplicated by Item ID, first-seen and
     published times kept so lead time and confirmation can be scored),
     _Econ_Events (US / SA releases upserted with estimate, actual and the
     surprise - the forecast-accuracy record the owner asked for). Evidence
     JSON + a markdown digest go to TFB_NEWS_GATE_EVIDENCE_DIR and the job
     summary.
OBSERVE-ONLY: nothing in the live system reads these tabs yet; the verdict
informs the owner's GO/NO-GO by hand until its rules are scored (a rule
earns enforcement only on measured value, owner decision).

GATE: TFB_NEWS_GATE = observe | on (armed) ; anything else = off (default):
no network call, exit 0. Without --write the run is a dry run: it collects
and prints the verdict, and writes nothing.
ENV: DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS (service account JSON
or base64), TFB_SEC_USER_AGENT (EDGAR contact; EDGAR is skipped without it),
EODHD_API_KEY (econ + market; skipped without it), TFB_NEWS_GATE_SOURCES
(comma list, default all), TFB_NEWS_GATE_SYMBOLS ("SYM[:Name];..."),
TFB_NEWS_GATE_MAX_NAMES (default 15), TFB_NEWS_GATE_MAX_ITEMS (default 200),
TFB_NEWS_GATE_GDELT_SPACING_S (default 6), TFB_NEWS_GATE_EVIDENCE_DIR.
Exit: 0 verdict computed (and written with --write); 1 no source answered or
a write failed; 2 configuration error.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import hashlib
import html
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, quote_plus, urlparse

__version__ = "1.13.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TAXONOMY_PATH = os.path.join(_REPO, "data", "news_gate_taxonomy.json")

SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"
SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)
GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GNEWS_RSS = "https://news.google.com/rss/search"
FED_RSS = "https://www.federalreserve.gov/feeds/press_all.xml"
EDGAR_TICKERS = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
EODHD_ECON = "https://eodhd.com/api/economic-events"
EODHD_RT = "https://eodhd.com/api/real-time/{code}"
BING_NEWS_RSS = "https://www.bing.com/news/search"                    # v1.5.0 [SOURCES]
YAHOO_RSS = "https://feeds.finance.yahoo.com/rss/2.0/headline"        # v1.5.0 [SOURCES]
FINNHUB_API = "https://finnhub.io/api/v1"                                # v1.6.0 [FINNHUB]
MARKETAUX_API = "https://api.marketaux.com/v1/news/all"                  # v1.7.0 [MARKETAUX]

# v1.5.0 [SOURCES]: five keyless sources beside the six of v1.0.0. FEED_GROUPS are read from FEEDS below.
NEW_SOURCES = ("bing_news", "yahoo_rss", "press_rss", "official_rss", "gulf_rss", "finnhub",   # v1.6.0: finnhub
               "marketaux", "eodhd_news")                                                     # v1.7.0, v1.13.0
FEED_GROUPS = ("press_rss", "official_rss", "gulf_rss")
ALL_SOURCES = ("edgar", "gdelt", "google_news", "fed_rss", "econ", "market") + NEW_SOURCES
# v1.13.0: read only when named in TFB_NEWS_GATE_SOURCES (Yahoo: 0 headlines inside the window on 26-27 Nov)
OFF_BY_DEFAULT = ("yahoo_rss",)
DEFAULT_SOURCES = tuple(s for s in ALL_SOURCES if s not in OFF_BY_DEFAULT)
EODHD_NEWS = "https://eodhd.com/api/news"                                # v1.13.0 [SOURCES-REVIEW]
EODHD_NEWS_LIMIT = 20
# v1.13.0: the Saudi financial sites an Arabic Google News search is held to (site: operators)
SAUDI_SITES = ("argaam.com", "mubasher.info", "aleqt.com", "alarabiya.net")
# v1.0.1: a source stops after 2 consecutive failed queries (circuit open) or
# once its time budget is spent, so one throttled provider cannot hold the job
# to its 20-minute limit (dry run 40595558188 sat in collection > 10 min).
# v1.9.0: official sites that ask a robot to name a contact for its owner (sec.gov's fair-access rule; the
# bls.gov terms reserve the right to block robots that give none)
CONTACT_DOMAINS = frozenset(["sec.gov", "bls.gov"])
# v1.9.0: waits before each try of the memory read (the first is immediate; a refusal with HTTP 429 gets the next)
MEMORY_RETRY_S = (0.0, 20.0, 40.0)
SOURCE_BUDGET_S = {"gdelt": 240.0, "google_news": 120.0, "edgar": 150.0,
                   "fed_rss": 45.0, "econ": 60.0, "market": 45.0,
                   "bing_news": 90.0, "yahoo_rss": 60.0, "press_rss": 75.0,
                   "official_rss": 75.0, "gulf_rss": 45.0, "finnhub": 45.0,
                   "marketaux": 30.0, "eodhd_news": 30.0}
BREAKER_FAILURES = 2
# v1.5.0 [SOURCES]: a feed is asked once per run and never held longer than this; a reply larger than
# MAX_FEED_BYTES is not parsed. Windows: what evaluate() can still use (24 h market-wide, 72 h for a name).
FEED_TIMEOUT_S = 12.0
MAX_FEED_BYTES = 4_000_000
MACRO_WINDOW_H = 24.0
NAME_WINDOW_H = 72.0
# (feed id, source group, URL, publisher domain - it sets the tier -, language, window in hours).
# Checked 26 Nov 2026. The Fed's own feed stays the source fed_rss.
FEEDS: Tuple[Tuple[str, str, str, str, str, float], ...] = (
    ("marketwatch", "press_rss", "https://feeds.content.dowjones.io/public/rss/mw_topstories",
     "marketwatch.com", "en", MACRO_WINDOW_H),
    ("wsj_markets", "press_rss", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
     "wsj.com", "en", MACRO_WINDOW_H),
    ("cnbc", "press_rss", "https://www.cnbc.com/id/100003114/device/rss/rss.html",
     "cnbc.com", "en", MACRO_WINDOW_H),
    ("bbc_business", "press_rss", "https://feeds.bbci.co.uk/news/business/rss.xml",
     "bbc.com", "en", MACRO_WINDOW_H),
    ("nyt_business", "press_rss", "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
     "nytimes.com", "en", MACRO_WINDOW_H),
    ("investing", "press_rss", "https://www.investing.com/rss/news.rss",
     "investing.com", "en", MACRO_WINDOW_H),
    ("seekingalpha", "press_rss", "https://seekingalpha.com/market_currents.xml",
     "seekingalpha.com", "en", MACRO_WINDOW_H),
    ("sec_press", "official_rss", "https://www.sec.gov/news/pressreleases.rss",
     "sec.gov", "en", NAME_WINDOW_H),
    ("bea", "official_rss", "https://apps.bea.gov/rss/rss.xml",
     "bea.gov", "en", NAME_WINDOW_H),
    ("bls", "official_rss", "https://www.bls.gov/feed/bls_latest.rss",
     "bls.gov", "en", NAME_WINDOW_H),
    ("ecb", "official_rss", "https://www.ecb.europa.eu/rss/press.html",
     "ecb.europa.eu", "en", NAME_WINDOW_H),
    ("eia", "official_rss", "https://www.eia.gov/rss/press_rss.xml",
     "eia.gov", "en", NAME_WINDOW_H),
    ("whitehouse_actions", "official_rss", "https://www.whitehouse.gov/presidential-actions/feed/",
     "whitehouse.gov", "en", NAME_WINDOW_H),
    # v1.11.0 [CENTRAL-BANKS]
    ("boe", "official_rss", "https://www.bankofengland.co.uk/rss/news",
     "bankofengland.co.uk", "en", NAME_WINDOW_H),
    ("boj", "official_rss", "https://www.boj.or.jp/en/rss/whatsnew.xml",
     "boj.or.jp", "en", NAME_WINDOW_H),
    ("snb", "official_rss", "https://www.snb.ch/public/rss/en/pressrel",
     "snb.ch", "en", NAME_WINDOW_H),
    ("asharq_business", "gulf_rss", "https://www.asharqbusiness.com/rss.xml",
     "asharqbusiness.com", "ar", MACRO_WINDOW_H),
    ("arabnews", "gulf_rss", "https://www.arabnews.com/rss.xml",
     "arabnews.com", "en", MACRO_WINDOW_H),
    ("thenational", "gulf_rss",
     "https://www.thenationalnews.com/arc/outboundfeeds/rss/category/business/?outputType=xml",
     "thenationalnews.com", "en", MACRO_WINDOW_H),
    ("aljazeera", "gulf_rss", "https://www.aljazeera.com/xml/rss/all.xml",
     "aljazeera.com", "en", MACRO_WINDOW_H),
    # v1.13.0 [SOURCES-REVIEW]: Arabic Saudi feeds, on trial - the first runs on main show whether the runner
    # may read them (Argaam's main news and market pulse were live on 26 Nov; Mubasher's TDWL feed answered a
    # page, not a feed, to a cloud reader). A feed that refuses or is not a feed is named and taken out.
    ("argaam_main", "gulf_rss", "https://www.argaam.com/ar/rss/ho-main-news?sectionid=1523",
     "argaam.com", "ar", MACRO_WINDOW_H),
    ("argaam_pulse", "gulf_rss", "https://www.argaam.com/ar/rss/ho-market-pulse?sectionid=70",
     "argaam.com", "ar", MACRO_WINDOW_H),
    ("mubasher_tdwl", "gulf_rss", "https://feeds.mubasher.info/ar/TDWL/news",
     "mubasher.info", "ar", MACRO_WINDOW_H),
)

GATE_TAB = "_News_Gate"
ITEMS_TAB = "_News_Items"
ECON_TAB = "_Econ_Events"
GATE_HEADER = ["Run At (Riyadh)", "Run ID", "Verdict", "Drivers", "Scheduled Events",
               "Market", "Names Flagged", "Items (new/classified/collected)",
               "Sources OK", "Sources Failed", "Mode", "Version"]
ITEMS_HEADER = ["Item ID", "First Seen (Riyadh)", "Published (UTC)", "Scope", "Event Type",
                "Direction", "Severity", "Rumour/Denial", "Source", "Domain", "Tier",
                "Cluster Grade", "Headline", "URL", "Cluster", "Run ID"]
ECON_HEADER = ["Key", "Date (UTC)", "Country", "Event", "Comparison", "Period", "Importance",
               "Estimate", "Actual", "Previous", "Surprise", "Surprise %", "Updated At (Riyadh)"]
# v1.0.2: headlines from earlier runs stay in the clusters while they can still
# move the verdict (evaluate(): 24 h for macro, 72 h for a name).
MEMORY_HOURS = {"MACRO": 24.0, "NAME": 72.0}
# v1.5.0 [SOURCES]: the new news sources are carried too; official feeds keep days of items and are re-read.
MEMORY_SOURCES = ("gdelt", "google_news", "bing_news", "yahoo_rss", "press_rss", "gulf_rss", "finnhub", "marketaux",
                  "eodhd_news")                                                          # v1.13.0
ISSUER_LINK_SOURCES = ("gdelt", "google_news", "bing_news", "yahoo_rss", "finnhub", "marketaux", "eodhd_news")
# v1.6.0 [FINNHUB]: the publisher of a Finnhub item whose URL is Finnhub's own redirect, read from its 'source'
FINNHUB_SOURCE_DOMAINS: Dict[str, str] = {
    "reuters": "reuters.com", "bloomberg": "bloomberg.com", "cnbc": "cnbc.com", "marketwatch": "marketwatch.com",
    "wsj": "wsj.com", "barrons": "barrons.com", "benzinga": "benzinga.com", "seekingalpha": "seekingalpha.com",
    "yahoo": "yahoo.com", "motley fool": "fool.com", "zacks": "zacks.com", "tipranks": "tipranks.com",
    "businesswire": "businesswire.com", "globenewswire": "globenewswire.com", "prnewswire": "prnewswire.com",
    "investing.com": "investing.com", "forbes": "forbes.com", "fortune": "fortune.com", "ft": "ft.com",
    "the fly": "thefly.com", "financial times": "ft.com",
}
_LEVEL_ORDER = {"BLOCK": 0, "WATCH": 1, "INFO": 2}

VERDICTS = ("GO", "CAUTION", "NO_NEW_ENTRY")

# Source tiers: A official / regulator / exchange / issuer wire; B licensed
# wire or specialist financial press; C mainstream; D aggregator; E social;
# F unknown (default). Matched on the registrable domain or a parent suffix.
SOURCE_TIERS: Dict[str, str] = {
    # A
    "sec.gov": "A", "federalreserve.gov": "A", "saudiexchange.sa": "A",
    "tadawul.com.sa": "A", "cma.org.sa": "A", "sama.gov.sa": "A", "spa.gov.sa": "A",
    "opec.org": "A", "eia.gov": "A", "bls.gov": "A", "bea.gov": "A",
    "bankofengland.co.uk": "A", "boj.or.jp": "A", "snb.ch": "A",      # v1.11.0
    "treasury.gov": "A", "ecb.europa.eu": "A", "imf.org": "A", "whitehouse.gov": "A",
    "globenewswire.com": "A", "prnewswire.com": "A", "businesswire.com": "A",
    "accessnewswire.com": "A", "newsfilecorp.com": "A",
    # B
    "reuters.com": "B", "bloomberg.com": "B", "wsj.com": "B", "ft.com": "B",
    "apnews.com": "B", "argaam.com": "B", "asharqbusiness.com": "B", "aleqt.com": "B",
    "barrons.com": "B", "economist.com": "B", "nikkei.com": "B", "benzinga.com": "B",
    "dowjones.com": "B", "spglobal.com": "B", "argusmedia.com": "B",
    # C
    "cnbc.com": "C", "marketwatch.com": "C", "bbc.com": "C", "bbc.co.uk": "C",
    "nytimes.com": "C", "washingtonpost.com": "C", "theguardian.com": "C",
    "aljazeera.net": "C", "aljazeera.com": "C", "alarabiya.net": "C", "arabnews.com": "C",
    "saudigazette.com.sa": "C", "zawya.com": "C", "gulfnews.com": "C",
    "thenationalnews.com": "C", "mubasher.info": "C", "investing.com": "C",
    "foxbusiness.com": "C", "cnn.com": "C", "axios.com": "C", "politico.com": "C",
    "okaz.com.sa": "C", "alriyadh.com": "C", "al-jazirah.com": "C", "sabq.org": "C",
    "aawsat.com": "C", "skynewsarabia.com": "C", "independent.co.uk": "C",
    "fortune.com": "C", "forbes.com": "C", "businessinsider.com": "C", "cnbcarabia.com": "C",
    # D
    "yahoo.com": "D", "msn.com": "D", "seekingalpha.com": "D", "fool.com": "D",
    "bing.com": "D",   # v1.5.0: a Bing News link whose publisher could not be read
    "finnhub.io": "D",  # v1.6.0: a Finnhub item whose publisher could not be read
    "eodhd.com": "D",   # v1.13.0: an EODHD item whose publisher could not be read
    "zacks.com": "D", "tipranks.com": "D", "marketbeat.com": "D", "news.google.com": "D",
    "insidermonkey.com": "D", "simplywall.st": "D", "stocktitan.net": "D",
    "gurufocus.com": "D", "finviz.com": "D", "investorsobserver.com": "D",
    # E
    "x.com": "E", "twitter.com": "E", "reddit.com": "E", "stocktwits.com": "E",
    "t.me": "E", "bsky.app": "E", "facebook.com": "E", "linkedin.com": "E",
}
_TIER_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5}
_TWO_LEVEL_SUFFIXES = ("com.sa", "gov.sa", "org.sa", "net.sa", "edu.sa", "co.uk",
                       "org.uk", "com.au", "co.jp", "com.hk", "com.eg", "co.za",
                       "or.jp", "go.jp")      # v1.11.0: boj.or.jp was read as "or.jp" (an unknown source)

# Anchors whose escalation can move this book (Gulf / oil / Fed / China).
MARKET_ANCHORS = frozenset([
    "hormuz", "iran", "israel", "lebanon", "yemen", "houthi", "saudi", "aramco", "opec",
    "fed", "china", "taiwan", "tariff",
    "\u0647\u0631\u0645\u0632", "\u0625\u064a\u0631\u0627\u0646",
    "\u0625\u0633\u0631\u0627\u0626\u064a\u0644", "\u0644\u0628\u0646\u0627\u0646",
    "\u0627\u0644\u064a\u0645\u0646", "\u0627\u0644\u062d\u0648\u062b\u064a",
    "\u0627\u0644\u0635\u064a\u0646", "\u0623\u0648\u0628\u0643",
    "\u0627\u0644\u0641\u064a\u062f\u0631\u0627\u0644\u064a",
    "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629", "\u0623\u0631\u0627\u0645\u0643\u0648",
])
GULF_ANCHORS = frozenset([
    "hormuz", "iran", "yemen", "houthi", "saudi", "aramco", "opec",
    "\u0647\u0631\u0645\u0632", "\u0625\u064a\u0631\u0627\u0646", "\u0627\u0644\u064a\u0645\u0646",
    "\u0627\u0644\u062d\u0648\u062b\u064a", "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
    "\u0623\u0631\u0627\u0645\u0643\u0648", "\u0623\u0648\u0628\u0643",
])

# FOMC decision days (second day of each meeting; 14:00 America/New_York).
# Source: federalreserve.gov calendar as summarised by fedratecalc.com, read
# 28 Oct 2026 (2027 is the Fed's tentative schedule); v1.4.1: 2027 as the Fed
# revised it on 12 Nov 2026 (no decision on 27 Oct; 8 Dec moved to 17 Nov).
FOMC_DECISION_DAYS = (
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17", "2026-07-29",
    "2026-09-16", "2026-10-28", "2026-12-09",
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-09", "2027-07-28",
    "2027-09-15", "2027-11-17",
)
FOMC_NAME_RE = re.compile(r"(fomc|interest rate decision|fed funds|federal funds|fed press conference|"
                          r"economic projections)", re.I)
HIGH_ECON_RE = re.compile(
    r"(nonfarm|non farm|payrolls|\bcpi\b|consumer price|inflation rate|interest rate decision|"
    r"fed funds|fomc|gdp growth|gdp annualized|\bpce\b|retail sales|unemployment rate|"
    r"ism manufacturing)", re.I)

# SEC 8-K items: (event_type, direction, severity, label)
EIGHT_K_ITEMS: Dict[str, Tuple[str, int, str, str]] = {
    "1.01": ("contract", 1, "L", "material agreement"),
    "1.02": ("contract", -1, "M", "agreement terminated"),
    "1.03": ("bankruptcy", -1, "H", "bankruptcy or receivership"),
    "1.05": ("legal_regulatory", -1, "M", "cybersecurity incident"),
    "2.01": ("mna", 0, "M", "acquisition or disposition completed"),
    "2.02": ("earnings", 0, "M", "results of operations"),
    "2.03": ("offering", 0, "L", "new debt obligation"),
    "2.04": ("credit_event", -1, "H", "debt acceleration trigger"),
    "2.05": ("restructuring", -1, "M", "exit or restructuring costs"),
    "2.06": ("impairment", -1, "M", "material impairment"),
    "3.01": ("delisting", -1, "H", "delisting or listing-rule notice"),
    "3.02": ("offering", -1, "L", "unregistered equity sale"),
    "4.01": ("auditor_change", -1, "M", "auditor change"),
    "4.02": ("restatement", -1, "H", "non-reliance on prior financials"),
    "5.01": ("mna", 0, "M", "change in control"),
    "5.02": ("management_change", 0, "L", "officer or director change"),
    "5.07": ("shareholder_vote", 0, "L", "shareholder vote"),
    "7.01": ("disclosure", 0, "L", "Regulation FD disclosure"),
    "8.01": ("disclosure", 0, "L", "other events"),
}
BLOCK_8K = frozenset(["1.03", "2.04", "3.01", "4.02"])
WATCH_8K = frozenset(["1.02", "1.05", "2.05", "2.06", "4.01"])
_SEV_RANK = {"H": 0, "M": 1, "L": 2}


# ----------------------------------------------------------------------------- #
# small helpers
# ----------------------------------------------------------------------------- #
# v1.8.1 [PUBLIC-LOG]: the scope's symbols, tickers and names to keep out of a public log (empty: print as is)
_PUBLIC_MASK: List[str] = []


def _out(msg: str) -> None:
    if _PUBLIC_MASK:
        msg = mask_public(msg, _PUBLIC_MASK)
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def log_detail() -> bool:
    """v1.8.1: may the run log name the scope's symbols? TFB_NEWS_GATE_LOG_DETAIL = on | off | auto (default:
    not on GitHub Actions, whose logs are public for this repository) - exit_plan 1.6.1's rule."""
    if (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() == "true":
        return False      # v1.12.0 (audit E1): the logs of this repository are public - never on Actions
    v = (os.environ.get("TFB_NEWS_GATE_LOG_DETAIL") or "auto").strip().lower()
    return v not in ("off", "0", "false", "no")


def mask_tokens(scope: Sequence[Dict[str, str]]) -> List[str]:
    """PURE: what a public log must not show - each scope symbol, its ticker (the part before the dot, two
    characters or more, letters - a numeric Saudi code alone would mask counts and ages, and its full symbol
    is masked) and its company name (four characters or more), longest first."""
    out = set()
    for s in scope:
        sym = str(s.get("symbol") or "").strip()
        if sym:
            out.add(sym)
            root = sym.split(".")[0]
            if len(root) >= 2 and not root.isdigit():
                out.add(root)
        name = str(s.get("name") or "").strip()
        if len(name) >= 4:
            out.add(name)
    return sorted(out, key=lambda t: (-len(t), t))


def _mask_values(obj: Any, tokens: Sequence[str]) -> Any:
    """PURE: obj with every string value masked; numbers, booleans and keys stay as they are."""
    if isinstance(obj, str):
        return mask_public(obj, tokens)
    if isinstance(obj, dict):
        return {k: _mask_values(v, tokens) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_mask_values(v, tokens) for v in obj]
    return obj


def mask_public(text: str, tokens: Sequence[str]) -> str:
    """PURE: text with every token replaced by "[name]", whole words only (a ticker inside a longer word stays)."""
    if not tokens or not text:
        return text
    pat = re.compile(r"(?<![A-Za-z0-9])(?:" + "|".join(re.escape(t) for t in tokens) + r")(?![A-Za-z0-9])")
    return pat.sub("[name]", text)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def gate_mode() -> str:
    v = _env("TFB_NEWS_GATE").lower()
    return v if v in ("observe", "on") else "off"


def _int_env(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(_env(name, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


def _float_env(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


def selected_sources(spec: str) -> List[str]:
    """v1.13.0: blank = DEFAULT_SOURCES (every source but OFF_BY_DEFAULT); a list names the sources to read."""
    if not spec.strip():
        return list(DEFAULT_SOURCES)
    want = [s.strip().lower() for s in spec.split(",") if s.strip()]
    return [s for s in ALL_SOURCES if s in want]


def item_id(url: str, title: str, source: str, scope: str = "") -> str:
    """One row per article AND scope: an article returned by a macro query
    and by a company query keeps both associations (Codex P2, #859)."""
    basis = (url or "").strip() or (source + "|" + (title or "").strip().lower())
    return hashlib.sha1((scope + "|" + basis).encode("utf-8")).hexdigest()[:16]


def domain_of(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in _TWO_LEVEL_SUFFIXES:
        return ".".join(parts[-3:])
    if host.endswith("news.google.com"):
        return "news.google.com"
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def tier_of(domain: str) -> str:
    d = (domain or "").lower()
    while d:
        if d in SOURCE_TIERS:
            return SOURCE_TIERS[d]
        if "." not in d:
            break
        d = d.split(".", 1)[1]
    return "F"


def best_tier(tiers: Iterable[str]) -> str:
    ts = [t for t in tiers if t in _TIER_RANK]
    return min(ts, key=lambda t: _TIER_RANK[t]) if ts else "F"


def fmt_riyadh(ts: _dt.datetime) -> str:
    return ts.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M:%S")


def fmt_utc(ts: Optional[_dt.datetime]) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M") if ts else ""


# ----------------------------------------------------------------------------- #
# taxonomy + classification
# ----------------------------------------------------------------------------- #
_PUNCT = re.compile(r"[\s\"'`.,;:!?()\[\]{}<>|/\\*#@%^&=+~\u060c\u061b\u061f\u00ab\u00bb\u201c\u201d\u2018\u2019-]+")


def load_taxonomy(path: Optional[str] = None) -> Dict[str, Any]:
    with open(path or TAXONOMY_PATH, "r", encoding="utf-8") as fh:
        tax = json.load(fh)
    if not isinstance(tax, dict) or not tax.get("events"):
        raise ValueError("taxonomy has no events")
    return tax


def normalize_text(text: str) -> str:
    t = _PUNCT.sub(" ", (text or "").lower())
    return " " + " ".join(t.split()) + " "


def _norm_terms(terms: Iterable[str]) -> List[str]:
    """Short ASCII words ("cpi", "fed", "eps") match whole words only; a
    trailing space in the taxonomy marks a word end (Arabic words keep their
    attached prefixes, e.g. al- or wa-, so only the end is bounded). A
    leading space marks a word start with an open end (v1.0.1): " stall"
    matches stalls / stalled but not installed, " fail" matches failed /
    failure."""
    out = []
    for term in terms or []:
        n = normalize_text(term).strip()
        if not n:
            continue
        if term.startswith(" "):
            out.append(" " + n + (" " if term.endswith(" ") else ""))
        elif all(ord(ch) < 128 for ch in n) and len(n) <= 4:
            out.append(" " + n + " ")
        elif term.endswith(" "):
            out.append(n + " ")
        else:
            out.append(n)
    return out


# v1.4.0 [TRUCE-READ]: "no ceasefire violations", "without a breach" - the breach word is denied, so the
# headline does not report a ceasefire as breaking. English only; one word may stand between.
_NO_BREACH_RE = re.compile(r"(?<= )(?:no|without|zero) (?:[^ ]+ )?(?:violat|breach)[^ ]*")


def _hit(norm: str, terms: Sequence[str]) -> bool:
    return any(t in norm for t in terms)


def _count(norm: str, terms: Sequence[str]) -> int:
    return sum(1 for t in terms if t in norm)


class Classifier:
    """Bilingual keyword classifier over data/news_gate_taxonomy.json."""

    def __init__(self, tax: Dict[str, Any]):
        self.events: List[Dict[str, Any]] = []
        for ev in tax.get("events") or []:
            self.events.append({
                "type": ev["type"], "scope": ev["scope"], "direction": int(ev["direction"]),
                "severity": ev["severity"],
                # v1.2.0: a label-only type names a subject and never enters a rule
                "label_only": ev.get("label_only") is True,
                "terms": _norm_terms(list(ev.get("en") or []) + list(ev.get("ar") or [])),
            })
        def pair(key: str) -> List[str]:
            block = tax.get(key) or {}
            return _norm_terms(list(block.get("en") or []) + list(block.get("ar") or []))
        self.by_type = {e["type"]: e for e in self.events}
        self.breach = pair("breach")
        # v1.3.0 [TRADE-RELIEF]: three optional lists; without them the two rules below are off
        self.truce = pair("trade_truce")
        self.relief = pair("trade_relief")
        self.hedge = pair("trade_hedge")
        rel = tax.get("trade_relief") or {}
        verbs = [v for v in (normalize_text(x).strip() for x in rel.get("verbs") or []) if v]
        objs = [o for o in (normalize_text(x).strip() for x in rel.get("objects") or []) if o]
        alt = "|".join(re.escape(v) for v in verbs)
        # group 1 holds the words between the verb and 'tariffs': they are kept when the match is set aside,
        # so a pressure word among them still counts ("lifts sanctions and tariffs" - Codex P2, #1033)
        self.relief_re = re.compile(r"(?<= )(?:%s) ((?:[^ ]+ ){0,2})(?:%s)(?= )" % (
            alt, "|".join(re.escape(o) for o in objs))) if verbs and objs else None
        # a relief verb in front of a relief phrase undoes it ("scraps tariff relief", "ends the tariff pause"),
        # and so does its past form after the phrase ("tariff pause lifted", "tariff cuts dropped")
        nouns = "|".join(re.escape(n.strip()) for n in self.relief if n.strip() and all(ord(ch) < 128 for ch in n))
        done = "|".join(re.escape(v) for v in verbs if v.split()[0].endswith("ed"))
        self.undo_re = re.compile(r"(?<= )(?:%s) (?:[^ ]+ ){0,2}(?:%s)" % (alt, nouns)
                                  + (r"|(?:%s)s? (?:[^ ]+ ){0,2}(?:%s)(?= )" % (nouns, done) if done else "")) \
            if verbs and nouns else None
        self.pos = pair("positive")
        self.neg = pair("negative")
        self.rumor = pair("rumor")
        self.denial = pair("denial")
        self.anchors = [(a, normalize_text(a).strip()) for a in tax.get("anchors") or []]
        # Arabic anchors map to the English name, so one story told in both
        # languages lands in one cluster (and one source count).
        self.canon = dict(tax.get("anchor_canon") or {})

    def anchor(self, norm: str) -> str:
        for raw, n in self.anchors:
            if n and (" " + n) in norm:
                return self.canon.get(raw, raw)
        return ""

    @staticmethod
    def _without(norm: str, terms: Sequence[str], pattern: Optional["re.Pattern[str]"] = None) -> str:
        """The normalised headline with the given phrases set aside (v1.3.0)."""
        out = pattern.sub(lambda m: " " + (m.group(1) or ""), norm) if pattern is not None else norm
        for t in terms:
            out = out.replace(t, " ")
        return " " + " ".join(out.split()) + " "

    def _trade_relabel(self, norm: str, title: str, found: Dict[str, Any]) -> str:
        """v1.3.0: 'truce' when the only ceasefire word of a de_escalation reading sits inside a trade-truce
        phrase; 'relief' when a trade_sanctions reading states tariff relief plainly; else ''. Both lead to the
        label-only type trade_deal, so the result can only take a reading OUT of a verdict rule."""
        trade = self.by_type.get("trade_deal")
        if trade is None or not trade["label_only"]:
            return ""
        if found["type"] == "de_escalation":
            if self.truce and _hit(norm, self.truce) and not _hit(self._without(norm, self.truce), found["terms"]):
                return "truce"
        elif found["type"] == "trade_sanctions":
            stated = _hit(norm, self.relief) or (self.relief_re is not None and self.relief_re.search(norm) is not None)
            hedged = _hit(norm, self.hedge) or _hit(norm, self.breach) or _hit(norm, self.denial) \
                or _hit(norm, self.rumor) or "?" in title or "\u061f" in title \
                or (self.undo_re is not None and self.undo_re.search(norm) is not None)
            if stated and not hedged and not _hit(self._without(norm, self.relief, self.relief_re), found["terms"]):
                return "relief"
        return ""

    def classify(self, title: str, scope: str) -> Dict[str, Any]:
        norm = normalize_text(title)
        company_first = scope != "MACRO"
        # v1.2.0: a name's own headline never takes a label-only type (a negative one would flag the name WATCH)
        order = ([e for e in self.events if e["scope"] == "COMPANY"]
                 + [e for e in self.events if e["scope"] == "MACRO" and not e["label_only"]]) if company_first \
            else [e for e in self.events if e["scope"] == "MACRO"]
        found = None
        for ev in order:
            if _hit(norm, ev["terms"]):
                found = ev
                break
        # A truce or de-escalation story that also names an attack word is
        # de-escalation unless it says the truce was broken (Codex P2, #859).
        deesc = self.by_type.get("de_escalation")
        # v1.4.0 [TRUCE-READ]: 'truce' inside a trade-truce phrase is not a ceasefire word here either, so
        # fighting beside a trade truce stays fighting.
        if found is not None and found["type"] == "war_escalation" and deesc is not None \
                and _hit(self._without(norm, self.truce) if self.truce else norm, deesc["terms"]) \
                and not _hit(norm, self.breach):
            found = deesc
        # v1.3.0 [TRADE-RELIEF]: a trade truce is not a ceasefire, a tariff cut is not trade pressure. The
        # reading moves to the label-only trade_deal; a name's own headline, which takes no label, loses it.
        relabel = self._trade_relabel(norm, title, found) if found is not None else ""
        if relabel:
            found = None if company_first else self.by_type["trade_deal"]
        pos, neg = _count(norm, self.pos), _count(norm, self.neg)
        if found is None:
            return {"event_type": "", "direction": 0, "severity": "",
                    "rumor": _hit(norm, self.rumor), "denial": _hit(norm, self.denial),
                    "anchor": self.anchor(norm)}
        direction = found["direction"]
        if relabel == "truce" and (_hit(norm, self.hedge) or _hit(norm, self.breach)):
            direction = -1                   # a trade truce in doubt: shown as falling, counted nowhere
        elif found["type"] == "de_escalation" and not company_first \
                and _hit(_NO_BREACH_RE.sub(" ", norm), self.breach):
            direction = -1                   # v1.4.0: a ceasefire reported as breaking is not easing
        elif direction == 0:
            direction = 1 if pos > neg else (-1 if neg > pos else 0)
        elif found["type"] in ("earnings", "guidance", "rating_change", "oil_supply", "central_bank"):
            direction = 1 if pos > neg else (-1 if neg > pos else direction)
        return {"event_type": found["type"], "direction": direction,
                "severity": found["severity"], "rumor": _hit(norm, self.rumor),
                "denial": _hit(norm, self.denial), "anchor": self.anchor(norm)}


# ----------------------------------------------------------------------------- #
# source parsers (pure functions; tests feed recorded payloads)
# ----------------------------------------------------------------------------- #
def _parse_gdelt_time(s: str) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return None


def parse_gdelt(payload: Any, scope: str, label: str) -> List[Dict[str, Any]]:
    arts = (payload or {}).get("articles") if isinstance(payload, dict) else None
    out = []
    for a in arts or []:
        url = str(a.get("url") or "")
        title = " ".join(str(a.get("title") or "").split())
        if not title:
            continue
        dom = (str(a.get("domain") or "") or domain_of(url)).lower()
        if dom.startswith("www."):
            dom = dom[4:]
        out.append({"source": "gdelt", "query": label, "scope": scope, "title": title,
                    "url": url, "domain": dom, "published": _parse_gdelt_time(a.get("seendate")),
                    "lang": str(a.get("language") or "").lower()[:2] or "en"})
    return out


def _rfc822(s: str) -> Optional[_dt.datetime]:
    try:
        d = parsedate_to_datetime(s)
        return d.astimezone(UTC) if d.tzinfo else d.replace(tzinfo=UTC)
    except (TypeError, ValueError, IndexError):
        return None


# v1.5.0 [SOURCES]: one reader for RSS 2.0, RSS 1.0 (RDF) and Atom.
_FEED_NS = ("http://www.w3.org/2005/Atom", "http://purl.org/rss/1.0/", "http://purl.org/dc/elements/1.1/")
_ENTITY_REF = re.compile(r"&(?:#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[A-Za-z][A-Za-z0-9]{1,31});")


def _local(tag: Any) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _feed_ns(tag: Any) -> str:
    return tag[1:].split("}", 1)[0] if isinstance(tag, str) and tag.startswith("{") else ""


def _feed_child(el: Any, names: Sequence[str]) -> Any:
    """The first direct child with one of `names`: the plain tag first, then that name in a feed namespace
    (so <media:title> never stands in for <title>)."""
    for name in names:
        for ch in el:
            if ch.tag == name:
                return ch
    for name in names:
        for ch in el:
            if _local(ch.tag) == name and _feed_ns(ch.tag) in _FEED_NS:
                return ch
    return None


def _feed_link(el: Any) -> str:
    spare = ""
    for ch in el:
        if _local(ch.tag) != "link" or (ch.tag != "link" and _feed_ns(ch.tag) not in _FEED_NS):
            continue
        href = (ch.get("href") or "").strip()
        if href:                                          # Atom: <link rel="alternate" href="..."/>
            if (ch.get("rel") or "alternate") == "alternate":
                return href
            spare = spare or href
        elif (ch.text or "").strip():
            return ch.text.strip()
    return spare


def _feed_time(text: Any) -> Optional[_dt.datetime]:
    """RFC 822 (RSS), ISO 8601 (Atom, dc:date) or 'YYYY-MM-DD HH:MM:SS'; a time with no zone is read as UTC."""
    raw = " ".join(str(text or "").split())
    if not raw:
        return None
    got = _rfc822(raw)
    if got is not None:
        return got
    try:
        d = _dt.datetime.fromisoformat(raw[:-1] + "+00:00" if raw[-1] in "Zz" else raw)
    except ValueError:
        return None
    return d.astimezone(UTC) if d.tzinfo else d.replace(tzinfo=UTC)


def _feed_root(payload: Any) -> Any:
    """The root of a feed reply, read from bytes when the reply has them (the parser then honours the
    feed's own encoding). Raises ValueError for a reply that is not a feed: an empty feed is a feed, a
    block page is not."""
    if isinstance(payload, (bytes, bytearray)):
        data: Any = bytes(payload)
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]
        data = data.lstrip()
        head = data[:60].decode("ascii", "replace")
        entity = b"<!ENTITY" in data
    else:
        data = str(payload or "")
        for mark in ("\ufeff", "\u00ef\u00bb\u00bf"):      # a byte-order mark, read as UTF-8 or as Latin-1
            if data.startswith(mark):
                data = data[len(mark):]
        data = data.lstrip()
        head = data[:60]
        entity = "<!ENTITY" in data
    head = " ".join(head.encode("ascii", "replace").decode("ascii").split())
    if not data:
        raise ValueError("empty reply")
    if len(data) > MAX_FEED_BYTES:
        raise ValueError(f"reply of {len(data)} characters is over the limit")
    if entity:
        raise ValueError("reply declares XML entities")
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise ValueError(f"not a feed ({str(exc)[:40]}): {head}")
    kind = _local(root.tag).lower()
    if kind not in ("rss", "feed", "rdf"):
        raise ValueError(f"not a feed (root <{kind[:20]}>)")
    return root


def parse_feed(payload: Any, scope: str, label: str, source: str, lang: str,
               domain: str = "") -> List[Dict[str, Any]]:
    """v1.5.0 [SOURCES]: the items of an RSS / Atom reply (text or bytes). `domain` names the publisher of a
    publisher's own feed; without it the item's <source url> or its link decides, as before. Raises
    ValueError when the reply is not a feed - parse_rss() below keeps the older, silent contract."""
    root = _feed_root(payload)
    out = []
    for it in root.iter():
        if it.tag not in ("item", "entry") and not (_local(it.tag) in ("item", "entry")
                                                    and _feed_ns(it.tag) in _FEED_NS):
            continue
        t = _feed_child(it, ("title",))
        # a feed that escapes twice leaves '&amp;' in the text; only whole references (ending ';') are undone
        title = " ".join(_ENTITY_REF.sub(lambda m: html.unescape(m.group(0)),
                                         (t.text or "") if t is not None else "").split())
        link = _feed_link(it)
        if not title:
            continue
        src = _feed_child(it, ("source",))
        src_url = (src.get("url") if src is not None else "") or ""
        src_name = (src.text or "").strip() if src is not None and src.text else ""
        if src_name and title.endswith(" - " + src_name):
            title = title[: -(len(src_name) + 3)].strip()
        dom = domain or (domain_of(src_url) if src_url else domain_of(link))
        when = _feed_child(it, ("pubDate", "published", "updated", "date"))
        out.append({"source": source, "query": label, "scope": scope, "title": title,
                    "url": link, "domain": dom, "published": _feed_time(when.text if when is not None else ""),
                    "lang": lang})
    return out


def parse_rss(xml_text: str, scope: str, label: str, source: str, lang: str) -> List[Dict[str, Any]]:
    """The lenient reader of v1.0.0, kept for its contract: [] for a reply that is not a feed. The
    collectors call parse_feed(), which says so."""
    try:
        return parse_feed(xml_text, scope, label, source, lang)
    except ValueError:
        return []


def _feed_payload(resp: Any) -> Any:
    raw = getattr(resp, "content", None)
    return raw if isinstance(raw, (bytes, bytearray)) and raw else getattr(resp, "text", "")


def bing_target(link: str) -> str:
    """v1.5.0 [SOURCES] PURE: the publisher's address inside a Bing News click link
    ('.../news/apiclick.aspx?...&url=<address>&...'); any other link is returned as it is."""
    try:
        p = urlparse(link)
        host = (p.hostname or "").lower()
        if host == "bing.com" or host.endswith(".bing.com"):
            target = (parse_qs(p.query).get("url") or [""])[0].strip()
            if target.lower().startswith(("http://", "https://")):
                return target
    except ValueError:
        pass
    return link


def finnhub_symbol(symbol: str) -> str:
    """v1.6.0 [FINNHUB] PURE: the ticker Finnhub's company news takes ('' = not asked: North American only)."""
    s = str(symbol or "").strip().upper()
    return s[:-3].replace("-", ".") if s.endswith(".US") and len(s) > 3 else ""


def parse_finnhub(payload: Any, scope: str, label: str) -> List[Dict[str, Any]]:
    """v1.6.0 [FINNHUB] PURE: items of a /news or /company-news reply (a JSON list). Raises ValueError when the
    reply is not that list, so a refusal is named under Sources Failed rather than counted as zero."""
    if isinstance(payload, dict):
        raise ValueError("not a news list: %s" % str(payload.get("error") or list(payload)[:3])[:60])
    if not isinstance(payload, list):
        raise ValueError("not a news list")
    out = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        title = " ".join(str(row.get("headline") or "").split())
        url = str(row.get("url") or "").strip()
        if not title:
            continue
        try:
            stamp = int(row.get("datetime") or 0)
        except (TypeError, ValueError):
            stamp = 0
        published = _dt.datetime.fromtimestamp(stamp, UTC) if stamp > 0 else None
        dom = domain_of(url)
        if not dom or dom == "finnhub.io":
            name = " ".join(str(row.get("source") or "").lower().split())
            dom = FINNHUB_SOURCE_DOMAINS.get(name, "finnhub.io")
        out.append({"source": "finnhub", "query": label, "scope": scope, "title": title, "url": url,
                    "domain": dom, "published": published, "lang": "en"})
    return out


def tadawul_code(symbol: str) -> str:
    """v1.13.0 [SOURCES-REVIEW] PURE: the four-digit Tadawul code of a '.SR' symbol ('' for any other)."""
    s = str(symbol or "").strip().upper()
    return s[:-3] if s.endswith(".SR") and s[:-3].isdigit() else ""


# Codex P2 on #1104: the Yahoo-style suffixes EODHD names differently - the table of
# archive_symbol_deep.EODHD_SUFFIX, the one news_now asks with (tests/test_news_macro_gate.py G23 holds them equal;
# that module is not imported here: it pulls in the archive's readers)
EODHD_NEWS_SUFFIX = {"NS": "NSE", "BO": "BSE", "DE": "XETRA", "AX": "AU", "KS": "KO",
                     "L": "LSE", "XETR": "XETRA", "ETR": "XETRA", "TASE": "TA"}


def eodhd_news_symbol(symbol: str) -> str:
    """v1.13.0 [SOURCES-REVIEW] PURE: the code EODHD's news takes for a scope symbol ('' = not asked: the Saudi
    names, which EODHD's company news does not cover - news_now reads holdings and the Top 10 the same way).
    The Yahoo-style suffixes EODHD names differently are translated (BARC.L -> BARC.LSE, SAP.DE -> SAP.XETRA)."""
    s = str(symbol or "").strip().upper()
    if not s or s == "MACRO" or s.endswith(".SR") or "." not in s:
        return ""
    base, suf = s.rsplit(".", 1)
    return f"{base}.{EODHD_NEWS_SUFFIX.get(suf, suf)}"


def parse_eodhd_news(payload: Any, scope: str, label: str) -> List[Dict[str, Any]]:
    """v1.13.0 [SOURCES-REVIEW] PURE: items of an EODHD /api/news reply (a JSON list). The sentiment fields are
    never read. Raises ValueError when the reply is not that list, so a refusal is named rather than counted as
    zero."""
    if isinstance(payload, dict):
        raise ValueError("not a news list: %s" % str(payload.get("message") or payload.get("error")
                                                     or list(payload)[:3])[:60])
    if not isinstance(payload, list):
        raise ValueError("not a news list")
    out = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        title = " ".join(str(row.get("title") or "").split())
        url = str(row.get("link") or "").strip()
        if not title:
            continue
        out.append({"source": "eodhd_news", "query": label, "scope": scope, "title": title, "url": url,
                    "domain": domain_of(url) or "eodhd.com", "published": _feed_time(row.get("date")),
                    "lang": "en"})
    return out


def parse_marketaux(payload: Any, scope: str, label: str) -> List[Dict[str, Any]]:
    """v1.7.0 [MARKETAUX] PURE: items of a /v1/news/all reply ({"meta": .., "data": [..]}). Raises ValueError for
    an error reply or any other shape, so a refusal is named rather than counted as zero."""
    if not isinstance(payload, dict):
        raise ValueError("not a news reply")
    if payload.get("error"):
        err = payload["error"]
        raise ValueError("refused: %s" % str(err.get("code") if isinstance(err, dict) else err)[:60])
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("not a news reply")
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = " ".join(str(row.get("title") or "").split())
        url = str(row.get("url") or "").strip()
        if not title:
            continue
        dom = domain_of(url) or domain_of("https://" + str(row.get("source") or "").strip().lower())
        out.append({"source": "marketaux", "query": label, "scope": scope, "title": title, "url": url,
                    "domain": dom, "published": _feed_time(row.get("published_at")),
                    "lang": str(row.get("language") or "en")[:2]})
    return out


def yahoo_symbol(symbol: str) -> str:
    """v1.5.0 [SOURCES] PURE: the code Yahoo Finance lists a scope symbol under ('' = not asked)."""
    s = str(symbol or "").strip().upper()
    if s.endswith(".US") and len(s) > 3:
        return s[:-3].replace(".", "-")
    return s if s.endswith(".SR") and len(s) > 3 else ""


def within_window(items: Sequence[Dict[str, Any]], now: _dt.datetime, hours: float
                  ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """v1.5.0 [SOURCES] PURE: (items published inside the last `hours`, how many were left out). An undated
    item is left out - evaluate() reads a missing date as 'now', so an old undated headline would count as
    fresh - and so is one dated more than an hour ahead of the run."""
    kept: List[Dict[str, Any]] = []
    left = {"outside": 0, "undated": 0}
    floor, ceiling = now - _dt.timedelta(hours=hours), now + _dt.timedelta(hours=1)
    for it in items:
        published = it.get("published")
        if published is None:
            left["undated"] += 1
        elif published < floor or published > ceiling:
            left["outside"] += 1
        else:
            kept.append(it)
    return kept, left


def fold_new_sources(items: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """v1.5.0 [SOURCES] PURE: (items kept, {source: copies left out}). One story from one publisher is one
    member of its cluster: an item of a NEW source is left out when an item of an older source carries the
    same headline from the same publisher in the same scope, or when another item of a new source claimed
    it first (a row recorded by an earlier run before a fresh copy; otherwise the order of collection).
    The same item met again (same Item ID) is not a copy. Items of the six older sources are never left
    out here, and an item with no publisher or no headline is not compared."""
    def key(it: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
        dom = str(it.get("domain") or "").lower()
        title = normalize_text(it.get("title") or "").strip()
        return (str(it.get("scope") or ""), dom, title) if dom and title else None

    def iid(it: Dict[str, Any]) -> str:
        return it.get("id") or item_id(it.get("url", ""), it.get("title", ""), it.get("source", ""),
                                       it.get("scope", ""))

    older = set()
    claimed: Dict[Tuple[str, str, str], str] = {}
    for it in items:
        k = key(it)
        if k is None:
            continue
        if it.get("source") not in NEW_SOURCES:
            older.add(k)
        elif it.get("memory"):
            claimed.setdefault(k, iid(it))
    kept: List[Dict[str, Any]] = []
    left: Dict[str, int] = {}
    for it in items:
        src = it.get("source")
        k = key(it) if src in NEW_SOURCES else None
        if k is not None:
            mine = iid(it)
            if k in older or claimed.setdefault(k, mine) != mine:
                left[src] = left.get(src, 0) + 1
                continue
        kept.append(it)
    return kept, left


def source_ages(items: Sequence[Dict[str, Any]], now: _dt.datetime) -> Dict[str, Dict[str, int]]:
    """v1.5.0 [SOURCES] PURE: per news source, the age in minutes at this run of its newest and its median
    dated item (rows carried from memory and EDGAR filings are not counted)."""
    ages: Dict[str, List[float]] = {}
    for it in items:
        if it.get("memory") or it.get("source") == "edgar" or not it.get("published"):
            continue
        ages.setdefault(str(it.get("source") or ""), []).append(
            max(0.0, (now - it["published"]).total_seconds() / 60.0))
    out: Dict[str, Dict[str, int]] = {}
    for src, xs in ages.items():
        xs.sort()
        half = len(xs) // 2
        median = xs[half] if len(xs) % 2 else (xs[half - 1] + xs[half]) / 2.0
        out[src] = {"n": len(xs), "newest_min": int(round(xs[0])), "median_min": int(round(median))}
    return out


def parse_ticker_map(payload: Any) -> Dict[str, Tuple[int, str]]:
    out: Dict[str, Tuple[int, str]] = {}
    rows = payload.values() if isinstance(payload, dict) else (payload or [])
    for row in rows:
        try:
            out[str(row["ticker"]).upper()] = (int(row["cik_str"]), str(row.get("title") or ""))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _edgar_time(row_date: str, accept: str) -> Optional[_dt.datetime]:
    for s, fmt in ((accept, "%Y-%m-%dT%H:%M:%S.%fZ"), (accept, "%Y-%m-%dT%H:%M:%SZ"),
                   (row_date, "%Y-%m-%d")):
        try:
            return _dt.datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
    return None


def parse_edgar_submissions(payload: Any, symbol: str, cik: int,
                            since: _dt.datetime) -> List[Dict[str, Any]]:
    rec = (((payload or {}).get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}
    forms = rec.get("form") or []
    out = []
    for i, form in enumerate(forms):
        if form not in ("8-K", "8-K/A", "6-K", "6-K/A"):
            continue
        def col(name: str) -> str:
            vals = rec.get(name) or []
            return str(vals[i]) if i < len(vals) and vals[i] is not None else ""
        when = _edgar_time(col("filingDate"), col("acceptanceDateTime"))
        if when is None or when < since:
            continue
        items = [x.strip() for x in col("items").split(",") if x.strip()]
        acc = col("accessionNumber")
        doc = col("primaryDocument")
        url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{doc}"
               if acc and doc else "")
        labels = [f"{x} {EIGHT_K_ITEMS[x][3]}" for x in items if x in EIGHT_K_ITEMS]
        title = f"{form} " + ("; ".join(labels) if labels else (", ".join(items) or "filing"))
        out.append({"source": "edgar", "query": "submissions", "scope": symbol, "title": title,
                    "url": url, "domain": "sec.gov", "published": when, "lang": "en",
                    "form": form, "items": items})
    return out


def parse_econ_events(payload: Any) -> List[Dict[str, Any]]:
    out = []
    for r in payload if isinstance(payload, list) else []:
        try:
            when = _dt.datetime.strptime(str(r.get("date")), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
        typ = str(r.get("type") or "").strip()
        if not typ:
            continue
        est, act, prev = r.get("estimate"), r.get("actual"), r.get("previous")
        surprise = surprise_pct = None
        if isinstance(est, (int, float)) and isinstance(act, (int, float)):
            surprise = round(act - est, 6)
            if est:
                surprise_pct = round((act - est) / abs(est) * 100.0, 2)
        country = str(r.get("country") or "").upper()
        comp = str(r.get("comparison") or "")
        period = str(r.get("period") or "")
        out.append({
            "key": "|".join([when.strftime("%Y-%m-%d %H:%M"), country, typ, comp, period]),
            "date": when, "country": country, "type": typ, "comparison": comp, "period": period,
            "importance": "HIGH" if HIGH_ECON_RE.search(typ) else "NORMAL",
            "estimate": est, "actual": act, "previous": prev,
            "surprise": surprise, "surprise_pct": surprise_pct,
        })
    return out


def parse_market(payload: Any) -> Dict[str, Dict[str, Any]]:
    rows = payload if isinstance(payload, list) else ([payload] if isinstance(payload, dict) else [])
    out = {}
    for r in rows:
        code = str((r or {}).get("code") or "")
        if not code:
            continue
        def num(k: str) -> Optional[float]:
            v = r.get(k)
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        ts = num("timestamp")          # v1.12.0: the quote's own time (EODHD real-time: Unix seconds)
        out[code] = {"close": num("close"), "previous_close": num("previousClose"),
                     "change_p": num("change_p"),
                     "as_of": _dt.datetime.fromtimestamp(ts, UTC) if ts and ts > 0 else None}
    return out


# ----------------------------------------------------------------------------- #
# clustering + grading
# ----------------------------------------------------------------------------- #
# v1.10.0 [ORIGINS]: a trailing " - Site" / " | Site" on a headline, and the least a headline must hold to be compared
_SITE_TAIL = re.compile(r"\s+[-|\u2013\u2014]\s+([^-|\u2013\u2014]{2,40})$")
ORIGIN_MIN_CHARS = 25


def _site_names() -> frozenset:
    """The names a headline's tail may carry as attribution: the first label of every graded domain, the
    publisher names Finnhub uses, and the two agencies known by their initials."""
    names = {d.split(".")[0] for d in SOURCE_TIERS} | {"".join(normalize_text(k).split()) for k in FINNHUB_SOURCE_DOMAINS}
    return frozenset(n for n in names | {"ap", "afp", "associatedpress"} if len(n) >= 2)


_SITE_NAMES = _site_names()


def is_site_tail(tail: str, domain: str = "") -> bool:
    """PURE: is this trailing " - ..." part of a headline a site's name? Yes when it names the item's own
    domain ("BBC News" on bbc.com), or is or begins with a known publisher's name ("Reuters", "CNBC TV18").
    Any other short clause ("- exports collapse") is part of the headline (Codex P2 on #1097)."""
    words = normalize_text(tail).split()
    if words[:1] == ["the"]:
        words = words[1:]
    squashed = "".join(words)
    if not squashed or len(words) > 4:
        return False
    label = str(domain or "").lower().split(".")[0]
    if len(label) >= 3 and (label in squashed or "".join(w[0] for w in words) == label):
        return True
    return squashed in _SITE_NAMES or words[0] in _SITE_NAMES or "".join(words[:2]) in _SITE_NAMES


MARKET_MAX_AGE_H = 96.0     # v1.12.0 (audit A6): a market quote older than this is not used (a long weekend fits)


def in_window(published: Any, since: _dt.datetime, now: _dt.datetime) -> bool:
    """PURE, v1.12.0 (audit A1): a member counts in a window only with a date, no older than `since` and no
    more than one hour ahead of the run (a clock off by minutes is tolerated, a stamp in the future is not)."""
    return isinstance(published, _dt.datetime) and since <= published <= now + _dt.timedelta(hours=1)


def origin_key(member: Dict[str, Any]) -> str:
    """PURE: the headline without a trailing site name, normalised; "" when it is too short to tell a copy
    from a coincidence (or absent). The tail is removed only when is_site_tail() says it is a site's name."""
    title = str(member.get("title") or "")
    m = _SITE_TAIL.search(title)
    if m and is_site_tail(m.group(1), str(member.get("domain") or "")):
        title = title[:m.start()]
    t = normalize_text(title).strip()
    return t if len(t) >= ORIGIN_MIN_CHARS else ""


def origins(members: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """PURE, v1.10.0: one member per origin, in first-seen order. Members with the same origin_key are one
    story reprinted; the copy from the best tier speaks for it (the first seen among equals). A member with
    no comparable headline is its own origin."""
    out: List[Dict[str, Any]] = []
    at: Dict[str, int] = {}
    for m in members:
        k = origin_key(m)
        if not k:
            out.append(m)
        elif k not in at:
            at[k] = len(out)
            out.append(m)
        elif _TIER_RANK.get(m.get("tier", "F"), 9) < _TIER_RANK.get(out[at[k]].get("tier", "F"), 9):
            out[at[k]] = m
    return out


def information_grade(members: Sequence[Dict[str, Any]]) -> int:
    """Admiralty-style: 1 confirmed .. 6 social only. v1.10.0: graded on the origins - a headline reprinted
    by several sites is one member."""
    members = origins(members)
    denials = [m for m in members if m.get("denial")]
    if denials:
        # A denial weighs against the story only when denials are at least
        # half of the cluster; otherwise the denying items are left out, so
        # an unrelated denial cannot wipe out a confirmed situation (Codex
        # P2, #859).
        if len(denials) * 2 >= len(members):
            return 5
        members = [m for m in members if not m.get("denial")]
    tiers = [m.get("tier", "F") for m in members]
    domains = {m.get("domain") for m in members if m.get("domain")}
    if tiers and all(t == "E" for t in tiers):
        return 6
    if "A" in tiers or (len(domains) >= 3 and "B" in tiers):
        grade = 1
    elif "B" in tiers or len([d for d in domains if tier_of(d) in ("B", "C")]) >= 2:
        grade = 2
    else:
        grade = 3
    if members and all(m.get("rumor") for m in members) and "A" not in tiers:
        grade = 4         # v1.12.0 (audit A3): also at grade 1 - three rumours are not a confirmation
    return grade


def cluster_items(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for it in items:
        if not it.get("event_type") or it["event_type"] == "law_firm_noise":
            continue
        key = (it["scope"], it["event_type"],
               (it.get("anchor") or "general") if it["scope"] == "MACRO" else "")
        groups.setdefault(key, []).append(it)
    clusters = []
    for (scope, etype, anchor), members in groups.items():
        dirs = [m.get("direction", 0) for m in origins(members)]     # v1.12.0 (audit A4): a copy votes once
        direction = 1 if sum(dirs) > 0 else (-1 if sum(dirs) < 0 else 0)
        sev = min((m.get("severity") or "L" for m in members), key=lambda s: _SEV_RANK.get(s, 3))
        pubs = [m["published"] for m in members if m.get("published")]
        cid = hashlib.sha1(f"{scope}|{etype}|{anchor}".encode("utf-8")).hexdigest()[:10]
        grade = information_grade(members)
        for m in members:
            m["cluster"] = cid
            m["cluster_grade"] = grade
        clusters.append({
            "id": cid, "scope": scope, "event_type": etype, "anchor": anchor,
            "members": members, "domains": sorted({m.get("domain") for m in members if m.get("domain")}),
            "tier": best_tier(m.get("tier", "F") for m in members), "grade": grade,
            "direction": direction, "severity": sev,
            "latest": max(pubs) if pubs else None,
            "headline": sorted(members, key=lambda m: _TIER_RANK.get(m.get("tier", "F"), 9))[0]["title"],
        })
    clusters.sort(key=lambda c: (_SEV_RANK.get(c["severity"], 3), c["grade"], -len(c["members"])))
    return clusters


# ----------------------------------------------------------------------------- #
# gate
# ----------------------------------------------------------------------------- #
def _ny_offset_hours(day: _dt.date) -> int:
    """US Eastern: -4 between the 2nd Sunday of March and the 1st Sunday of
    November, else -5 (fixed rule since 2007)."""
    def nth_sunday(year: int, month: int, n: int) -> _dt.date:
        d = _dt.date(year, month, 1)
        d += _dt.timedelta(days=(6 - d.weekday()) % 7)
        return d + _dt.timedelta(days=7 * (n - 1))
    start, end = nth_sunday(day.year, 3, 2), nth_sunday(day.year, 11, 1)
    return -4 if start <= day < end else -5


def fomc_decision_times() -> List[_dt.datetime]:
    out = []
    for s in FOMC_DECISION_DAYS:
        d = _dt.date.fromisoformat(s)
        out.append(_dt.datetime(d.year, d.month, d.day, 14, 0,
                                tzinfo=_dt.timezone(_dt.timedelta(hours=_ny_offset_hours(d)))).astimezone(UTC))
    return out


def scheduled_events(now: _dt.datetime, econ: Sequence[Dict[str, Any]],
                     ahead_h: float = 30.0, behind_h: float = 2.0) -> List[Dict[str, Any]]:
    out = []
    for t in fomc_decision_times():
        if now - _dt.timedelta(hours=behind_h) <= t <= now + _dt.timedelta(hours=ahead_h):
            out.append({"when": t, "country": "US", "event": "FOMC rate decision", "source": "fed calendar"})
    seen = {("US", "FOMC rate decision", e["when"].date()) for e in out}
    for e in econ:
        if e["importance"] != "HIGH" or e["country"] != "US":
            continue
        if now - _dt.timedelta(hours=behind_h) <= e["date"] <= now + _dt.timedelta(hours=ahead_h):
            name = e["type"]
            # v1.0.1: EODHD names the same meeting "FOMC Statement", "Fed
            # Interest Rate Decision", "FOMC Press Conference"... - one driver
            # per decision day (live dry run 40595558188 listed it twice).
            if FOMC_NAME_RE.search(name) and ("US", "FOMC rate decision", e["date"].date()) in seen:
                continue
            out.append({"when": e["date"], "country": e["country"], "event": name, "source": "eodhd"})
    out.sort(key=lambda x: x["when"])
    return out


def evaluate(clusters: Sequence[Dict[str, Any]], econ: Sequence[Dict[str, Any]],
             market: Dict[str, Dict[str, Any]], now: _dt.datetime) -> Dict[str, Any]:
    """Deterministic, observe-only verdict. Rules (v1.0.0):
    NO_NEW_ENTRY - S&P 500 day change <= -3% or VIX >= 35; or a Gulf-anchored
      high-severity macro story (war escalation / emergency policy / market
      turmoil) graded 1 (an A source, or >= 3 domains including a licensed
      wire) from >= 3 independent domains in the last 24 h.
    CAUTION - a FOMC decision or a high-impact US release within the next
      30 h or the last 2 h; S&P <= -1.5%, VIX >= 25 or VIX +15% on the day;
      any market-anchored high-severity macro story graded 1-2 from >= 3
      domains, or Gulf-anchored graded 1-4 (v1.9.0: confirmed by one or two
      domains counts too - more confirmation never lowers the verdict); a
      confirmed (grade <= 2) sanctions / credit-stress story on a market anchor.
    Clusters are situations (event type + anchor within the window), not
    single stories: several incidents around one Gulf anchor in 24 h are one
    situation by design (see the #859 review thread).
    Otherwise GO. Per name: BLOCK (8-K 1.03 / 2.04 / 3.01 / 4.02 within 30
    days, or a bankruptcy / delisting / restatement story graded <= 2),
    WATCH (8-K 1.02 / 1.05 / 2.05 / 2.06 / 4.01 within 10 days, or a negative
    story graded <= 2 in 72 h), INFO (8-K 2.02 within 10 days)."""
    level = 0
    drivers: List[str] = []
    positives: List[str] = []

    def bump(to: int, why: str) -> None:
        nonlocal level
        level = max(level, to)
        drivers.append(why)

    sched = scheduled_events(now, econ)
    for s in sched:
        when = fmt_riyadh(s["when"])[11:16]
        day = "today" if s["when"].astimezone(RIYADH).date() == now.astimezone(RIYADH).date() else s["when"].astimezone(RIYADH).strftime("%a %d %b")
        bump(1, f"scheduled: {s['event']} ({day} {when} Riyadh)")

    stale: List[str] = []

    def quote(code: str) -> Dict[str, Any]:
        """v1.12.0 (audit A6): a quote older than MARKET_MAX_AGE_H is not used, and is named."""
        q = market.get(code) or {}
        at = q.get("as_of")
        if isinstance(at, _dt.datetime) and now - at > _dt.timedelta(hours=MARKET_MAX_AGE_H):
            stale.append(f"{code} quote of {fmt_utc(at)} UTC")
            return {}
        return q

    spx = quote("GSPC.INDX")
    vix = quote("VIX.INDX")
    spx_chg, vix_lvl, vix_chg = spx.get("change_p"), vix.get("close"), vix.get("change_p")
    if spx_chg is not None and spx_chg <= -3.0:
        bump(2, f"market: S&P 500 {spx_chg:+.1f}% on the day")
    elif spx_chg is not None and spx_chg <= -1.5:
        bump(1, f"market: S&P 500 {spx_chg:+.1f}% on the day")
    if vix_lvl is not None and vix_lvl >= 35:
        bump(2, f"market: VIX {vix_lvl:.1f}")
    elif (vix_lvl is not None and vix_lvl >= 25) or (vix_chg is not None and vix_chg >= 15):
        bump(1, f"market: VIX {vix_lvl if vix_lvl is not None else float('nan'):.1f} ({(vix_chg or 0):+.0f}%)")

    since = now - _dt.timedelta(hours=24)
    for c in clusters:
        if c["scope"] != "MACRO":
            continue
        recent = [m for m in c["members"] if in_window(m.get("published"), since, now)]     # v1.12.0 (A1)
        if not recent:
            continue
        sites = {m.get("domain") for m in recent if m.get("domain")}
        doms = {m.get("domain") for m in origins(recent) if m.get("domain")}     # v1.10.0: copies count once
        grade = information_grade(recent)
        anchor = c["anchor"]
        tag = (f"{c['event_type']}/{anchor} grade {grade}, {len(sites)} sources"
               + (f" ({len(doms)} independent)" if len(doms) != len(sites) else "") + f": {c['headline'][:90]}")
        if c["event_type"] == "de_escalation":
            # v1.4.0 [TRUCE-READ]: a ceasefire reported as breaking (a member read as falling) is not easing.
            # The line lists the cluster for its other members only: they must outnumber the falling ones and
            # be confirmed on their own, and one of their headlines is quoted. A de-escalation cluster raises
            # nothing, as before.
            falling = [m for m in origins(recent) if m.get("direction", 0) < 0]     # v1.12.0: over origins
            if not falling:
                if grade <= 2:
                    positives.append(tag)
            else:
                calm = [m for m in origins(recent) if m.get("direction", 0) >= 0]
                if len(calm) > len(falling) and information_grade(calm) <= 2:
                    lead = sorted(calm, key=lambda m: _TIER_RANK.get(m.get("tier", "F"), 9))[0]["title"]
                    positives.append(f"{c['event_type']}/{anchor} grade {information_grade(calm)}, "
                                     f"{len({m.get('domain') for m in calm if m.get('domain')})} sources: {lead[:90]}")
            continue
        if c["severity"] == "H" and c["event_type"] in ("war_escalation", "emergency_policy", "market_turmoil"):
            if anchor in GULF_ANCHORS and grade == 1 and len(doms) >= 3:
                bump(2, "macro: " + tag)
            elif anchor in MARKET_ANCHORS and grade <= 2 and len(doms) >= 3:
                bump(1, "macro: " + tag)
            elif anchor in GULF_ANCHORS and grade <= 4:
                # v1.9.0 [REVIEW-26NOV]: grade 1-2 from one or two domains used to raise nothing
                bump(1, ("macro (unconfirmed): " if grade >= 3 else "macro (few sources): ") + tag)
        elif c["event_type"] in ("trade_sanctions", "credit_stress") and anchor in MARKET_ANCHORS \
                and grade <= 2 and len(doms) >= 2:
            bump(1, "macro: " + tag)

    names: Dict[str, List[Dict[str, str]]] = {}

    def flag(sym: str, lvl: str, why: str) -> None:
        names.setdefault(sym, []).append({"level": lvl, "why": why})

    for c in clusters:
        if c["scope"] == "MACRO":
            continue
        sym = c["scope"]
        for m in c["members"]:
            if m.get("source") != "edgar" or not m.get("published"):
                continue
            age_d = (now - m["published"]).total_seconds() / 86400.0
            items = set(m.get("items") or [])
            hit_block = sorted(items & BLOCK_8K)
            hit_watch = sorted(items & WATCH_8K)
            if hit_block and age_d <= 30:
                flag(sym, "BLOCK", f"{m['form']} item {', '.join(hit_block)} ({age_d:.0f}d ago)")
            elif hit_watch and age_d <= 10:
                flag(sym, "WATCH", f"{m['form']} item {', '.join(hit_watch)} ({age_d:.0f}d ago)")
            if "2.02" in items and age_d <= 10:
                flag(sym, "INFO", f"earnings released ({m['form']} 2.02, {age_d:.0f}d ago)")
        news = [m for m in c["members"] if m.get("source") != "edgar"
                and in_window(m.get("published"), now - _dt.timedelta(hours=72), now)]     # v1.12.0 (A1)
        if not news:
            continue
        grade = information_grade(news)
        if c["event_type"] in ("bankruptcy", "delisting", "restatement") and grade <= 2:
            flag(sym, "BLOCK", f"{c['event_type']} grade {grade}: {c['headline'][:80]}")
        elif c["direction"] < 0 and grade <= 2:
            flag(sym, "WATCH", f"{c['event_type']} negative, grade {grade}: {c['headline'][:80]}")

    for sym in list(names):
        seen, uniq = set(), []
        for f in names[sym]:
            k = (f["level"], f["why"])
            if k not in seen:
                seen.add(k)
                uniq.append(f)
        names[sym] = uniq
    return {"verdict": VERDICTS[level], "drivers": drivers, "positives": positives,
            "scheduled": sched, "market": {"spx_change_p": spx_chg, "vix": vix_lvl, "vix_change_p": vix_chg,
                                           "stale": stale},
            "names": names}


# ----------------------------------------------------------------------------- #
# HTTP collectors (injected session: .get(url, params=, headers=, timeout=))
# ----------------------------------------------------------------------------- #
class _Guard:
    """v1.0.1: per-source circuit breaker + time budget."""

    def __init__(self, col: "Collector", src: str):
        self.col, self.src = col, src
        self.t0 = col.clock()
        self.budget = SOURCE_BUDGET_S.get(src, 120.0)
        self.consecutive = 0
        self.skipped = 0
        self.open = False

    def allow(self) -> bool:
        if self.open or (self.col.clock() - self.t0) >= self.budget:
            self.skipped += 1
            return False
        return True

    def success(self) -> None:
        self.consecutive = 0

    def failure(self) -> None:
        self.consecutive += 1
        if self.consecutive >= BREAKER_FAILURES:
            self.open = True

    def close(self) -> None:
        if not self.skipped:
            return
        why = (f"circuit open after {BREAKER_FAILURES} consecutive failures" if self.open
               else f"time budget {self.budget:.0f}s spent")
        prev = self.col.failed.get(self.src, "")
        self.col.failed[self.src] = (((prev[:110] + "; ") if prev else "")
                                     + f"{why}; {self.skipped} queries skipped")[:240]


class Collector:
    def __init__(self, http: Any, sleep: Callable[[float], None], now: _dt.datetime,
                 clock: Callable[[], float] = time.monotonic):
        self.http, self.sleep, self.now, self.clock = http, sleep, now, clock
        self.timeout = _float_env("TFB_NEWS_GATE_TIMEOUT_S", 20.0, 5.0, 60.0)
        self.ok: Dict[str, int] = {}
        self.failed: Dict[str, str] = {}
        self.detail: Dict[str, Dict[str, int]] = {}      # v1.5.0 [SOURCES]: what each new source read and left out

    def _get(self, src: str, url: str, *, params: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None, retry_wait: float = 8.0,
             attempts: int = 2, timeout: Optional[float] = None) -> Any:
        last = max(1, attempts)
        for attempt in range(1, last + 1):
            try:
                resp = self.http.get(url, params=params, headers=headers or {}, timeout=timeout or self.timeout)
            except Exception as exc:  # network error
                if attempt == last:
                    raise RuntimeError(f"{type(exc).__name__}: {exc}")
                self.sleep(retry_wait)
                continue
            code = getattr(resp, "status_code", 0)
            if code == 200:
                return resp
            if code in (429, 500, 502, 503, 504) and attempt < last:
                self.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP {code}")
        raise RuntimeError("unreachable")

    def _note(self, src: str, n: int) -> None:
        self.ok[src] = self.ok.get(src, 0) + n

    def _fail(self, src: str, why: str) -> None:
        self.failed.setdefault(src, why[:160])

    def _fail_part(self, src: str, part: str, why: str) -> None:
        """v1.5.0 [SOURCES]: every feed or search of a new source that failed is named, not only the first."""
        prev = self.failed.get(src, "")
        self.failed[src] = (((prev + "; ") if prev else "") + f"{part}: {why}"[:72])[:420]
        self._tally(src, failed=1)

    def _tally(self, src: str, **counts: int) -> None:
        row = self.detail.setdefault(src, {})
        for k, v in counts.items():
            row[k] = row.get(k, 0) + int(v)

    def _read_feed(self, src: str, url: str, *, scope: str, label: str, lang: str, hours: float,
                   domain: str = "", params: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """v1.5.0 [SOURCES]: one feed, asked once (the next run asks again). Raises on a transport fault and
        on a reply that is not a feed; returns the items inside the window."""
        resp = self._get(src, url, params=params, headers=headers, attempts=1,
                         timeout=min(self.timeout, FEED_TIMEOUT_S))
        got = parse_feed(_feed_payload(resp), scope, label, src, lang, domain)
        kept, left = within_window(got, self.now, hours)
        self._tally(src, asked=1, read=len(got), outside=left["outside"], undated=left["undated"])
        return kept

    # -- GDELT ------------------------------------------------------------------
    def gdelt(self, queries: Sequence[Tuple[str, str, str]], spacing: float,
              timespan: str = "24h") -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "gdelt")
        for i, (scope, label, q) in enumerate(queries):
            if not guard.allow():
                continue
            if i:
                self.sleep(spacing)
            try:
                resp = self._get("gdelt", GDELT_API, params={
                    "query": q, "mode": "artlist", "format": "json", "maxrecords": "75",
                    "timespan": timespan if scope == "MACRO" else "3d", "sort": "datedesc"},
                    retry_wait=max(10.0, spacing * 2))
                try:
                    payload = resp.json()
                except ValueError:
                    # GDELT answers a malformed query with a plain-text message
                    # (the service answered: not a breaker failure)
                    self._fail("gdelt", f"{label}: non-JSON reply: {str(getattr(resp, 'text', ''))[:100]}")
                    guard.success()
                    continue
                got = parse_gdelt(payload, scope, label)
                out.extend(got)
                self._note("gdelt", len(got))
                guard.success()
            except Exception as exc:
                self._fail("gdelt", f"{label}: {exc}")
                guard.failure()
        guard.close()
        return out

    # -- Google News RSS ---------------------------------------------------------
    def google_news(self, queries: Sequence[Tuple[str, str, str, str]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "google_news")
        for i, (scope, label, q, lang) in enumerate(queries):
            if not guard.allow():
                continue
            if i:
                self.sleep(1.0)
            params = {"ar": {"q": q, "hl": "ar", "gl": "SA", "ceid": "SA:ar"},
                      "en-sa": {"q": q, "hl": "en", "gl": "SA", "ceid": "SA:en"}}.get(
                lang, {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"})
            try:
                resp = self._get("google_news", GNEWS_RSS, params=params)
                try:
                    got = parse_feed(_feed_payload(resp), scope, label, "google_news", lang[:2])
                except ValueError as exc:
                    # v1.5.0: a reply that is not a feed is named, not counted as zero items. The service
                    # answered, so - as with GDELT's plain-text replies - the breaker does not advance and
                    # the next search is still asked: this is the gate's main source.
                    self._fail("google_news", f"{label}: {exc}")
                    guard.success()
                    continue
                out.extend(got)
                self._note("google_news", len(got))
                guard.success()
            except Exception as exc:
                self._fail("google_news", f"{label}: {exc}")
                guard.failure()
        guard.close()
        return out

    # -- Fed RSS ----------------------------------------------------------------
    def fed_rss(self) -> List[Dict[str, Any]]:
        try:
            resp = self._get("fed_rss", FED_RSS)
            # v1.5.0: read from the reply's bytes; a reply that is not a feed is named under Sources Failed
            got = [i for i in parse_feed(_feed_payload(resp), "MACRO", "fed press", "fed_rss", "en")
                   if not i.get("published") or i["published"] >= self.now - _dt.timedelta(days=3)]
            for i in got:
                i["domain"] = "federalreserve.gov"
            self._note("fed_rss", len(got))
            return got
        except Exception as exc:
            self._fail("fed_rss", str(exc))
            return []

    # -- v1.5.0 [SOURCES] ---------------------------------------------------------
    def bing_news(self, queries: Sequence[Tuple[str, str, str]]) -> List[Dict[str, Any]]:
        """Bing News RSS, newest first. Each item is filed under its publisher (the address inside Bing's
        click link), so it is tiered and counted like the same story from any other source."""
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "bing_news")
        for i, (scope, label, q) in enumerate(queries):
            if not guard.allow():
                continue
            if i:
                self.sleep(1.0)
            try:
                got = self._read_feed("bing_news", BING_NEWS_RSS, scope=scope, label=label, lang="en",
                                      hours=MACRO_WINDOW_H if scope == "MACRO" else NAME_WINDOW_H,
                                      params={"q": q, "format": "rss", "qft": 'sortbydate="1"'})
                for it in got:
                    it["url"] = bing_target(it.get("url") or "")
                    it["domain"] = domain_of(it["url"])
                out.extend(got)
                self._note("bing_news", len(got))
                guard.success()
            except Exception as exc:
                self._fail_part("bing_news", label, str(exc))
                guard.failure()
        guard.close()
        return out

    def yahoo_rss(self, symbols: Sequence[str]) -> List[Dict[str, Any]]:
        """Yahoo Finance headline feed, one request per scope name Yahoo lists (US ticker or .SR code)."""
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "yahoo_rss")
        asked = 0
        for sym in symbols:
            code = yahoo_symbol(sym)
            if not code or not guard.allow():
                continue
            if asked:
                self.sleep(0.5)
            asked += 1
            try:
                got = self._read_feed("yahoo_rss", YAHOO_RSS, scope=sym, label=f"name {sym} yahoo", lang="en",
                                      hours=NAME_WINDOW_H, params={"s": code, "region": "US", "lang": "en-US"})
                out.extend(got)
                self._note("yahoo_rss", len(got))
                guard.success()
            except Exception as exc:
                self._fail_part("yahoo_rss", sym, str(exc))
                guard.failure()
        guard.close()
        return out

    def finnhub(self, symbols: Sequence[str], key: str) -> List[Dict[str, Any]]:
        """v1.6.0 [FINNHUB]: market news, then company news per US scope name. The key is sent as a header."""
        if not key:
            self._fail("finnhub", "FINNHUB_API_KEY not set")
            return []
        hdr = {"X-Finnhub-Token": key}
        frm = (self.now - _dt.timedelta(days=3)).strftime("%Y-%m-%d")
        to = self.now.strftime("%Y-%m-%d")
        asks: List[Tuple[str, str, str, Dict[str, Any], float]] = [
            ("MACRO", "market news finnhub", FINNHUB_API + "/news", {"category": "general"}, MACRO_WINDOW_H)]
        for sym in symbols:
            tick = finnhub_symbol(sym)
            if tick:
                asks.append((sym, f"name {sym} finnhub", FINNHUB_API + "/company-news",
                             {"symbol": tick, "from": frm, "to": to}, NAME_WINDOW_H))
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "finnhub")
        for i, (scope, label, url, params, hours) in enumerate(asks):
            if not guard.allow():
                continue
            if i:
                self.sleep(1.1)                          # 60 calls a minute on the free plan
            try:
                resp = self._get("finnhub", url, params=params, headers=hdr, attempts=1,
                                 timeout=min(self.timeout, FEED_TIMEOUT_S))
                try:
                    payload = resp.json()
                except ValueError:
                    raise ValueError("reply is not JSON")
                got = parse_finnhub(payload, scope, label)
                kept, left = within_window(got, self.now, hours)
                self._tally("finnhub", asked=1, read=len(got), outside=left["outside"], undated=left["undated"])
                out.extend(kept)
                self._note("finnhub", len(kept))
                guard.success()
            except Exception as exc:
                self._fail_part("finnhub", label, _scrub(str(exc), key))
                guard.failure()
        guard.close()
        return out

    def marketaux(self, symbols: Sequence[str], key: str, max_names: int) -> List[Dict[str, Any]]:
        """v1.7.0 [MARKETAUX]: company news for the first `max_names` scope names it lists (holdings come first
        in the scope). The free plan gives 100 requests a day, so the count per run is bounded."""
        if not key:
            self._fail("marketaux", "MARKETAUX_API_KEY not set")
            return []
        after = (self.now - _dt.timedelta(hours=NAME_WINDOW_H)).strftime("%Y-%m-%dT%H:%M:%S")
        asks = [(sym, yahoo_symbol(sym)) for sym in symbols]
        asks = [(sym, code) for sym, code in asks if code][:max(0, int(max_names))]
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "marketaux")
        for i, (sym, code) in enumerate(asks):
            if not guard.allow():
                continue
            if i:
                self.sleep(0.5)
            try:
                resp = self._get("marketaux", MARKETAUX_API, attempts=1, timeout=min(self.timeout, FEED_TIMEOUT_S),
                                 params={"api_token": key, "symbols": code, "filter_entities": "true",
                                         "language": "en", "published_after": after})
                try:
                    payload = resp.json()
                except ValueError:
                    raise ValueError("reply is not JSON")
                got = parse_marketaux(payload, sym, f"name {sym} marketaux")
                kept, left = within_window(got, self.now, NAME_WINDOW_H)
                self._tally("marketaux", asked=1, read=len(got), outside=left["outside"], undated=left["undated"])
                out.extend(kept)
                self._note("marketaux", len(kept))
                guard.success()
            except Exception as exc:
                self._fail_part("marketaux", sym, _scrub(str(exc), key))
                guard.failure()
        guard.close()
        return out

    def eodhd_news(self, symbols: Sequence[str], key: str) -> List[Dict[str, Any]]:
        """v1.13.0 [SOURCES-REVIEW]: EODHD's company news for every scope name but the Saudi ones (the call
        news_now makes every hour). The key is a query parameter, so every message is scrubbed of it."""
        if not key:
            self._fail("eodhd_news", "EODHD_API_KEY not set")
            return []
        frm = (self.now - _dt.timedelta(hours=NAME_WINDOW_H)).strftime("%Y-%m-%d")
        asks = [(sym, eodhd_news_symbol(sym)) for sym in symbols]
        asks = [(sym, code) for sym, code in asks if code]
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, "eodhd_news")
        for i, (sym, code) in enumerate(asks):
            if not guard.allow():
                continue
            if i:
                self.sleep(0.3)
            try:
                resp = self._get("eodhd_news", EODHD_NEWS, attempts=1, timeout=min(self.timeout, FEED_TIMEOUT_S),
                                 params={"s": code, "from": frm, "limit": EODHD_NEWS_LIMIT, "offset": 0,
                                         "api_token": key, "fmt": "json"})
                try:
                    payload = resp.json()
                except ValueError:
                    raise ValueError("reply is not JSON")
                got = parse_eodhd_news(payload, sym, f"name {sym} eodhd")
                kept, left = within_window(got, self.now, NAME_WINDOW_H)
                self._tally("eodhd_news", asked=1, read=len(got), outside=left["outside"], undated=left["undated"])
                out.extend(kept)
                self._note("eodhd_news", len(kept))
                guard.success()
            except Exception as exc:
                self._fail_part("eodhd_news", sym, _scrub(str(exc), key))
                guard.failure()
        guard.close()
        return out

    def feeds(self, group: str, sec_user_agent: str = "") -> List[Dict[str, Any]]:
        """The feeds of one group in FEEDS, each on its own: a feed that fails is named and the next one is
        read (they are different publishers, so there is no circuit breaker - only the group's time budget).
        sec.gov and (v1.9.0) bls.gov are asked with the declared contact and not at all without it."""
        out: List[Dict[str, Any]] = []
        guard = _Guard(self, group)
        for fid, grp, url, domain, lang, hours in FEEDS:
            if grp != group or not guard.allow():
                continue
            headers = None
            if domain in CONTACT_DOMAINS:
                if not sec_user_agent:
                    self._fail_part(group, fid, "TFB_SEC_USER_AGENT not set")
                    continue
                headers = {"User-Agent": sec_user_agent, "Accept-Encoding": "gzip, deflate"}
            try:
                got = self._read_feed(group, url, scope="MACRO", label=fid, lang=lang, hours=hours,
                                      domain=domain, headers=headers)
                out.extend(got)
                self._note(group, len(got))
            except Exception as exc:
                self._fail_part(group, fid, str(exc))
        if guard.skipped:      # the feeds already named stay named (the guard's own note keeps 110 characters)
            prev = self.failed.get(group, "")
            self.failed[group] = (((prev + "; ") if prev else "")
                                  + f"time budget {guard.budget:.0f}s spent: {guard.skipped} feeds not asked")[:420]
        return out

    # -- EDGAR ------------------------------------------------------------------
    def edgar(self, symbols: Sequence[str], user_agent: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        names: Dict[str, str] = {}
        if not user_agent:
            self._fail("edgar", "TFB_SEC_USER_AGENT not set")
            return [], names
        hdr = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        try:
            tmap = parse_ticker_map(self._get("edgar", EDGAR_TICKERS, headers=hdr).json())
        except Exception as exc:
            self._fail("edgar", f"ticker map: {exc}")
            return [], names
        out: List[Dict[str, Any]] = []
        since = self.now - _dt.timedelta(days=30)
        guard = _Guard(self, "edgar")
        for sym in symbols:
            if not sym.upper().endswith(".US"):
                continue
            tick = sym[:-3].upper().replace(".", "-")
            hit = tmap.get(tick) or tmap.get(tick.replace("-", "."))
            if not hit:
                continue
            cik, title = hit
            names[sym] = title
            if not guard.allow():
                continue
            self.sleep(0.15)
            try:
                payload = self._get("edgar", EDGAR_SUBMISSIONS.format(cik=cik), headers=hdr).json()
                got = parse_edgar_submissions(payload, sym, cik, since)
                out.extend(got)
                self._note("edgar", len(got))
                guard.success()
            except Exception as exc:
                self._fail("edgar", f"{sym}: {exc}")
                guard.failure()
        guard.close()
        if "edgar" not in self.ok and "edgar" not in self.failed:
            self.ok["edgar"] = 0
        return out, names

    # -- EODHD ------------------------------------------------------------------
    def econ(self, key: str) -> List[Dict[str, Any]]:
        if not key:
            self._fail("econ", "EODHD_API_KEY not set")
            return []
        out: List[Dict[str, Any]] = []
        frm = (self.now - _dt.timedelta(days=2)).strftime("%Y-%m-%d")
        to = (self.now + _dt.timedelta(days=3)).strftime("%Y-%m-%d")
        for country in ("US", "SA"):
            try:
                resp = self._get("econ", EODHD_ECON, params={
                    "api_token": key, "fmt": "json", "from": frm, "to": to,
                    "country": country, "limit": "1000"})
                got = parse_econ_events(resp.json())
                out.extend(got)
                self._note("econ", len(got))
            except Exception as exc:
                self._fail("econ", f"{country}: {_scrub(str(exc), key)}")
        return out

    def market(self, key: str) -> Dict[str, Dict[str, Any]]:
        if not key:
            self._fail("market", "EODHD_API_KEY not set")
            return {}
        try:
            resp = self._get("market", EODHD_RT.format(code="GSPC.INDX"), params={
                "api_token": key, "fmt": "json", "s": "VIX.INDX"})
            got = parse_market(resp.json())
            self._note("market", len(got))
            return got
        except Exception as exc:
            self._fail("market", _scrub(str(exc), key))
            return {}


def _scrub(text: str, secret: str) -> str:
    return text.replace(secret, "***") if secret else text


# ----------------------------------------------------------------------------- #
# queries
# ----------------------------------------------------------------------------- #
GDELT_MACRO_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("geopolitics", '(missile OR airstrike OR "military strike" OR invasion OR "Strait of Hormuz" OR Houthi OR ceasefire OR escalation) sourcelang:english'),
    ("oil", '(OPEC OR "oil prices" OR "crude oil" OR "Brent crude" OR "oil supply") sourcelang:english'),
    ("central banks", '("Federal Reserve" OR FOMC OR "rate cut" OR "rate hike" OR "Treasury yields" OR "interest rates") sourcelang:english'),
    ("market stress", '("stock market" OR "Wall Street" OR stocks) (plunge OR selloff OR "sell-off" OR crash OR "circuit breaker" OR rout) sourcelang:english'),
    ("saudi", '("Saudi Arabia" OR Tadawul OR "Saudi Aramco" OR "Saudi stocks" OR Riyadh) sourcelang:english'),
    # v1.2.0 [TOPICS]
    ("trade china", '(tariff OR "trade deal" OR "trade truce" OR "export controls" OR summit OR APEC OR "Xi Jinping") sourcelang:english'),
    ("ai tech", '("artificial intelligence" OR "AI bubble" OR Nvidia OR OpenAI OR "chip export" OR "chip stocks") sourcelang:english'),
    ("politics", '(election OR "government shutdown" OR "debt ceiling" OR "executive order") sourcelang:english'),
)
# Arabic Google-News queries (escaped: the repository keeps added lines ASCII)
GNEWS_AR_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("saudi market ar", "\u062a\u062f\u0627\u0648\u0644 OR \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629 when:1d"),
    ("oil ar", "\u0623\u0633\u0639\u0627\u0631 \u0627\u0644\u0646\u0641\u0637 OR \u0623\u0648\u0628\u0643 when:1d"),
    ("rates ar", "\u0627\u0644\u0641\u064a\u062f\u0631\u0627\u0644\u064a OR \u0633\u0639\u0631 \u0627\u0644\u0641\u0627\u0626\u062f\u0629 when:1d"),
    ("geopolitics ar", "\u0647\u062c\u0648\u0645 OR \u062a\u0635\u0639\u064a\u062f OR \u0645\u0636\u064a\u0642 \u0647\u0631\u0645\u0632 when:1d"),
    # v1.2.0 [TOPICS]
    ("trade china ar", "\u0642\u0645\u0629 OR \u0627\u0644\u0631\u0633\u0648\u0645 \u0627\u0644\u062c\u0645\u0631\u0643\u064a\u0629 OR \u0627\u062a\u0641\u0627\u0642 \u062a\u062c\u0627\u0631\u064a OR \u0627\u0644\u062d\u0631\u0628 \u0627\u0644\u062a\u062c\u0627\u0631\u064a\u0629 when:1d"),
    ("ai tech ar", "\u0627\u0644\u0630\u0643\u0627\u0621 \u0627\u0644\u0627\u0635\u0637\u0646\u0627\u0639\u064a OR \u0625\u0646\u0641\u064a\u062f\u064a\u0627 OR \u0623\u0634\u0628\u0627\u0647 \u0627\u0644\u0645\u0648\u0635\u0644\u0627\u062a when:1d"),
)
# v1.0.2: English Google-News (US edition) reaches the licensed wires when
# GDELT throttles the runner (27 Oct: circuit open at its second query).
GNEWS_EN_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("geopolitics en", '"Strait of Hormuz" OR Iran OR Houthi OR missile OR ceasefire when:1d'),
    ("oil en", '"oil prices" OR OPEC OR "Brent crude" OR "crude oil" when:1d'),
    ("rates en", '"Federal Reserve" OR FOMC OR "Treasury yields" OR "rate cut" when:1d'),
    ("markets en", '"Wall Street" OR "stock market" OR "Saudi stocks" OR Tadawul when:1d'),
    # v1.2.0 [TOPICS]
    ("trade china en", 'tariff OR "trade deal" OR "trade truce" OR "export controls" OR summit OR APEC when:1d'),
    ("ai tech en", '"artificial intelligence" OR "AI bubble" OR Nvidia OR OpenAI OR "chip stocks" when:1d'),
    ("politics en", 'election OR "government shutdown" OR "debt ceiling" OR "executive order" when:1d'),
)


# v1.1.0 [ISSUER-LINK]
_TICKER_PREFIXES = ("NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM", "NDQ", "NYSE", "NYSEAMERICAN", "NYSEARCA", "AMEX",
                    "OTC", "OTCMKTS", "LSE", "LON", "ENXTPA", "ENXTAM", "ENXTBR", "EPA", "AMS", "BME", "HKG", "HKEX",
                    "TSX", "ASX", "BMV", "TADAWUL", "XETRA", "ETR", "FRA")
_BROKER_VERBS = ("maintains", "raises", "lowers", "adjusts", "upgrades", "downgrades", "initiates", "reiterates",
                 "cuts", "sees", "turns", "rates", "resumes", "reinstates", "lifts", "trims", "boosts")
# Codex P2 (#1006): a broker role needs analyst-note words in the headline; 'Profit falls at Merchants Bancorp'
# and 'Barclays raises dividend' are about the issuer.
_ANALYST_NOTE = re.compile(r"\b(?:price target|target price|PT|ratings?|upgrade[sd]?|downgrade[sd]?|overweight|"
                           r"underweight|equal[- ]weight|outperform|underperform|market perform|sector perform|"
                           r"neutral|buy|sell|hold|initiat\w*|coverage|reiterat\w*)\b", re.I)


def _name_core(name: str) -> str:
    cleaned = re.sub(r"\b(inc|corp|corporation|co|company|ltd|plc|holdings?|group|sa|nv|ag|se|ord|l\.?p|s\.a)\b\.?",
                     " ", name or "", flags=re.I)
    return " ".join(cleaned.replace(",", " ").replace(".", " ").split())


def issuer_linked(title: str, symbol: str, name: str) -> Optional[bool]:
    """v1.1.0 [ISSUER-LINK] PURE: is this headline about the issuer of `symbol`? None = cannot judge (no name)."""
    t = " ".join(str(title or "").split())
    parts = str(symbol or "").upper().split(".")
    base, venue = parts[0], (parts[1] if len(parts) > 1 else "")
    if base:
        tick = re.escape(base)
        prefixes = "|".join(_TICKER_PREFIXES)
        own = (r"(?:\." + re.escape(venue) + r")?") if venue else ""
        # 'DHT.UN' is another listing: a ticker followed by a different '.XX' does not link
        if re.search(r"(?:\(|\$|\b(?:" + prefixes + r")\s*:\s*)" + tick + own + r"(?![A-Za-z0-9]|\.[A-Za-z])", t):
            return True
    core = _name_core(name)
    if not core:
        return None
    words = [re.escape(w) for w in core.split()]
    pat = re.compile(r"(?<![A-Za-z0-9])" + r"[\s\-]+".join(words) + r"(?![A-Za-z0-9]|\.[A-Za-z])", re.I)
    note = bool(_ANALYST_NOTE.search(t))
    for m in pat.finditer(t):
        before, after = t[:m.start()], t[m.end():]
        if re.search(r"&\s*$", before):
            continue                                   # 'Farmers & Merchants Bancorp': another company
        if note and re.search(r"\b(?:by|at|from)\s*$", before, re.I):
            continue                                   # '... Price Target Raised by Barclays': the broker
        if note and re.match(r"\s*(?:" + "|".join(_BROKER_VERBS) + r")\b", after, re.I):
            continue                                   # 'BNP Paribas raises price target for ...'
        return True
    return False


def _issuer_link_mode() -> str:
    v = _env("TFB_NEWS_ISSUER_LINK", "on").strip().lower()
    return v if v in ("on", "observe", "off") else "on"


def apply_issuer_link(items: List[Dict[str, Any]], names: Dict[str, str], mode: str
                      ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """v1.1.0 [ISSUER-LINK]: (kept items, report). Only the name items of the search and per-name sources
    (ISSUER_LINK_SOURCES: GDELT, Google News and, from v1.5.0, Bing News and Yahoo) are judged."""
    info: Dict[str, Any] = {"mode": mode, "checked": 0, "unlinked": 0, "unchecked": 0, "unnamed": 0,
                            "examples": []}
    unnamed = set()                                  # v1.12.1 (audit A2): counted, never named in the report
    if mode == "off":
        return items, info
    kept = []
    for it in items:
        scope = str(it.get("scope") or "")
        if scope and scope != "MACRO" and it.get("source") in ISSUER_LINK_SOURCES:
            verdict = issuer_linked(it.get("title", ""), scope, names.get(scope, ""))
            if verdict is None:                      # v1.12.1 (audit A2): kept, but counted and marked
                info["unchecked"] += 1
                unnamed.add(scope)
                it["issuer_checked"] = False
            if verdict is not None:
                info["checked"] += 1
                if verdict is False:
                    info["unlinked"] += 1
                    if len(info["examples"]) < 5:
                        info["examples"].append(f"{scope}: {str(it.get('title', ''))[:120]}")
                    if mode == "on":
                        continue
        kept.append(it)
    info["unnamed"] = len(unnamed)
    return kept, info


def _name_query(name: str) -> str:
    cleaned = re.sub(r"\b(inc|corp|corporation|co|company|ltd|plc|holdings?|group|sa|nv|ag|se)\b\.?",
                     " ", name, flags=re.I)
    cleaned = " ".join(cleaned.replace(",", " ").split())
    return f'"{cleaned}"' if cleaned else ""


def saudi_sites_query(terms: str, when: str) -> str:
    """v1.13.0 [SOURCES-REVIEW] PURE: an Arabic Google News search held to SAUDI_SITES."""
    return f"{terms} ({' OR '.join('site:' + d for d in SAUDI_SITES)}) when:{when}"


def build_queries(scope: Sequence[Dict[str, str]]) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str, str]]]:
    gdelt = [("MACRO", label, q) for label, q in GDELT_MACRO_QUERIES]
    gnews = [("MACRO", label, q, "ar") for label, q in GNEWS_AR_QUERIES]
    # v1.13.0: the Saudi market from the Saudi financial sites, in Arabic (the Asharq feed refuses the runner)
    gnews.append(("MACRO", "saudi sites ar", saudi_sites_query(
        "\u062a\u062f\u0627\u0648\u0644 OR \u062a\u0627\u0633\u064a OR \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629", "1d"), "ar"))
    gnews += [("MACRO", label, q, "en") for label, q in GNEWS_EN_QUERIES]
    # v1.13.0: SAMA has no feed; its own site through Google News (kept only if it gives dated items)
    gnews.append(("MACRO", "sama site", "site:sama.gov.sa when:3d", "en-sa"))
    for s in scope:
        code = tadawul_code(s.get("symbol") or "")
        if code:                                   # v1.13.0: a Saudi name in Arabic, by its code, on the Saudi sites
            gnews.append((s["symbol"], f"name {s['symbol']} ar", saudi_sites_query(f'"{code}"', "3d"), "ar"))
        name = s.get("name") or ""
        nq = _name_query(name)
        if not nq:
            continue
        if s["symbol"].upper().endswith(".SR"):
            gnews.append((s["symbol"], f"name {s['symbol']}", f"{nq} when:3d", "en-sa"))
        else:
            gdelt.append((s["symbol"], f"name {s['symbol']}", nq))
            gnews.append((s["symbol"], f"name {s['symbol']} en", f"{nq} when:3d", "en"))
    return gdelt, gnews


def build_bing_queries(scope: Sequence[Dict[str, str]]) -> List[Tuple[str, str, str]]:
    """v1.5.0 [SOURCES]: Google News' English macro searches without its 'when:' operator (Bing has none:
    the feed is asked newest first and the window is applied to the dates), then every scope name."""
    out = []
    for label, q in GNEWS_EN_QUERIES:
        out.append(("MACRO", (label[:-3] if label.endswith(" en") else label) + " bing",
                    re.sub(r"\s+when:\d+[dh]\s*$", "", q)))
    for s in scope:
        nq = _name_query(s.get("name") or "")
        if nq:
            out.append((s["symbol"], f"name {s['symbol']} bing", nq))
    return out


# ----------------------------------------------------------------------------- #
# Google Sheets (injected AuthorizedSession-like session)
# ----------------------------------------------------------------------------- #
def _credentials_info() -> Optional[Dict[str, Any]]:
    raw = _env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS")
    if not raw:
        return None
    try:
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8")
        info = json.loads(raw)
        return info if isinstance(info, dict) else None
    except Exception:
        return None


def make_sheets_session():  # pragma: no cover - needs google-auth and a real key
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    info = _credentials_info()
    if not info:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
    creds = service_account.Credentials.from_service_account_info(info, scopes=list(SCOPES))
    return AuthorizedSession(creds)


def make_http_session():  # pragma: no cover - needs requests
    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": f"TFB-news-gate/{__version__}"})
    return s


def _rng(tab: str, cells: str = "") -> str:
    return quote("'" + tab.replace("'", "''") + "'" + (("!" + cells) if cells else ""), safe="")


def read_values(sheets: Any, sid: str, tab: str, cells: str = "") -> Optional[List[List[Any]]]:
    resp = sheets.get(f"{SHEETS_API}/{sid}/values/{_rng(tab, cells)}?majorDimension=ROWS"
                      "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING",
                      timeout=120)
    if resp.status_code == 400:
        return None  # tab missing
    if resp.status_code != 200:
        raise RuntimeError(f"read {tab}: HTTP {resp.status_code}")
    return list((resp.json() or {}).get("values") or [])


def ensure_tab(sheets: Any, sid: str, tab: str, header: Sequence[str]) -> None:
    vals = read_values(sheets, sid, tab, "A1:Z1")
    if vals is None:
        resp = sheets.post(f"{SHEETS_API}/{sid}:batchUpdate", json={
            "requests": [{"addSheet": {"properties": {"title": tab}}}]}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"create {tab}: HTTP {resp.status_code}")
        vals = []
    if not vals or not vals[0]:
        resp = sheets.put(f"{SHEETS_API}/{sid}/values/{_rng(tab, 'A1')}?valueInputOption=RAW",
                          json={"values": [list(header)]}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"header {tab}: HTTP {resp.status_code}")


def append_rows(sheets: Any, sid: str, tab: str, rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        return
    resp = sheets.post(f"{SHEETS_API}/{sid}/values/{_rng(tab, 'A1')}:append"
                       "?valueInputOption=RAW&insertDataOption=INSERT_ROWS",
                       json={"values": [list(r) for r in rows]}, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"append {tab}: HTTP {resp.status_code}")


def update_rows(sheets: Any, sid: str, tab: str, updates: Sequence[Tuple[int, Sequence[Any]]]) -> None:
    if not updates:
        return
    data = [{"range": "'" + tab.replace("'", "''") + f"'!A{row}", "values": [list(vals)]}
            for row, vals in updates]
    resp = sheets.post(f"{SHEETS_API}/{sid}/values:batchUpdate",
                       json={"valueInputOption": "RAW", "data": data}, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"update {tab}: HTTP {resp.status_code}")


def _header_index(rows: Sequence[Sequence[Any]], wanted: str) -> Tuple[int, int]:
    for r, row in enumerate(rows[:10]):
        for c, cell in enumerate(row):
            if str(cell).strip().lower() == wanted.lower():
                return r, c
    return -1, -1


def read_scope(sheets: Any, sid: str, max_names: int) -> List[Dict[str, str]]:
    """Holdings first, then the latest _Shadow_Rank_Log day (order kept)."""
    out: List[Dict[str, str]] = []
    seen = set()

    def add(sym: str, name: str, role: str) -> None:
        sym = (sym or "").strip().upper()
        if not sym or sym in seen or not re.match(r"^[A-Z0-9][A-Z0-9.\-]*\.[A-Z]{1,4}$", sym):
            return
        seen.add(sym)
        out.append({"symbol": sym, "name": (name or "").strip(), "role": role})

    pf = read_values(sheets, sid, "My_Portfolio", "A1:B200") or []
    hr, sc = _header_index(pf, "Symbol")
    if hr >= 0:
        for row in pf[hr + 1:]:
            if len(row) > sc and row[sc]:
                add(str(row[sc]), str(row[sc + 1]) if len(row) > sc + 1 else "", "holding")
    log = read_values(sheets, sid, "_Shadow_Rank_Log", "A:H") or []
    hr, sc = _header_index(log, "Symbol")
    _, dc = _header_index(log, "Day")
    if hr >= 0 and dc >= 0:
        rows = [r for r in log[hr + 1:] if len(r) > max(sc, dc) and r[sc]]
        days = sorted({str(r[dc]) for r in rows})
        if days:
            for r in rows:
                if str(r[dc]) == days[-1]:
                    add(str(r[sc]), "", "board")
    names: Dict[str, str] = {}
    for tab in ("Market_Leaders", "Global_Markets"):
        vals = read_values(sheets, sid, tab, "A:B") or []
        h, c = _header_index(vals, "Symbol")
        if h < 0:
            continue
        for row in vals[h + 1:]:
            if len(row) > c + 1 and row[c]:
                names.setdefault(str(row[c]).strip().upper(), str(row[c + 1]).strip())
    for s in out:
        if not s["name"]:
            s["name"] = names.get(s["symbol"], "")
    return out[:max_names]


def parse_symbols_env(spec: str) -> List[Dict[str, str]]:
    out = []
    for part in [p for p in (spec or "").split(";") if p.strip()]:
        sym, _, name = part.partition(":")
        if sym.strip():
            out.append({"symbol": sym.strip().upper(), "name": name.strip(), "role": "env"})
    return out


# ----------------------------------------------------------------------------- #
# run
# ----------------------------------------------------------------------------- #
def classify_all(items: List[Dict[str, Any]], clf: Classifier, now: _dt.datetime) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        # v1.0.2: a carried item keeps its recorded Item ID and first-seen
        # time; fresh items come first, so a fresh copy wins on the same ID.
        iid = it.get("id") or item_id(it.get("url", ""), it.get("title", ""), it.get("source", ""),
                                      it.get("scope", ""))
        if iid in seen:
            continue
        seen.add(iid)
        it["id"] = iid
        it["tier"] = "A" if it.get("source") in ("edgar", "fed_rss") else tier_of(it.get("domain", ""))
        if not it.get("first_seen"):
            it["first_seen"] = now
        if it.get("source") == "edgar":
            worst = None
            for code in it.get("items") or []:
                meta = EIGHT_K_ITEMS.get(code)
                if meta and (worst is None or _SEV_RANK[meta[2]] < _SEV_RANK[worst[2]]):
                    worst = meta
            it.update({"event_type": worst[0] if worst else "disclosure",
                       "direction": worst[1] if worst else 0, "severity": worst[2] if worst else "L",
                       "rumor": False, "denial": False, "anchor": ""})
        else:
            it.update(clf.classify(it.get("title", ""), it.get("scope", "MACRO")))
        out.append(it)
    return out


def pick_new(classified: Sequence[Dict[str, Any]], existing: Iterable[str], max_items: int
             ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """PURE, v1.9.0: the classified items to record this run - not carried from memory, not recorded before;
    the items about names first, then the most severe and best graded; at most max_items. Also how many
    wait for a later run, and how many of those are about names."""
    done = {str(x) for x in existing}
    fresh = [i for i in classified if not i.get("memory") and i["id"] not in done]
    fresh.sort(key=lambda i: (0 if i.get("scope") != "MACRO" else 1,
                              _SEV_RANK.get(i.get("severity") or "L", 3), i.get("cluster_grade") or 9))
    kept, left = fresh[:max_items], fresh[max_items:]
    return kept, {"left": len(left), "left_names": len([i for i in left if i.get("scope") != "MACRO"])}


def item_row(it: Dict[str, Any], run_id: str) -> List[Any]:
    rd = "denial" if it.get("denial") else ("rumour" if it.get("rumor") else "")
    return [it["id"], fmt_riyadh(it["first_seen"]), fmt_utc(it.get("published")), it["scope"],
            it.get("event_type", ""), it.get("direction", 0), it.get("severity", ""), rd,
            it.get("source", ""), it.get("domain", ""), it.get("tier", "F"),
            it.get("cluster_grade", ""), (it.get("title") or "")[:250], (it.get("url") or "")[:500],
            it.get("cluster", ""), run_id]


def _parse_stamp(text: Any, tz: _dt.timezone) -> Optional[_dt.datetime]:
    raw = str(text or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return _dt.datetime.strptime(raw, fmt).replace(tzinfo=tz).astimezone(UTC)
        except ValueError:
            continue
    return None


def read_memory(sheets: Any, sid: str, now: _dt.datetime, max_rows: int,
                sources: Sequence[str] = MEMORY_SOURCES,
                symbols: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """v1.0.2: headlines of the news sources (MEMORY_SOURCES; v1.5.0 adds the new
    ones) recorded by earlier runs that can
    still move the verdict (published within 24 h for macro, 72 h for a
    name), read from the newest max_rows rows of _News_Items. EDGAR, the Fed
    feed and the official feeds are re-read whole every run, so their rows are not carried, and
    a source left out of the run's selection is not carried either. With
    symbols given, a name's rows are carried only for names in that scope."""
    allowed = {src for src in sources if src in MEMORY_SOURCES}
    names = None if symbols is None else {str(x).strip().upper() for x in symbols}
    if max_rows <= 0 or not allowed:
        return []
    ids = read_values(sheets, sid, ITEMS_TAB, "A2:A")
    if not ids:
        return []
    last = len(ids) + 1
    first = max(2, last - max_rows + 1)
    rows = read_values(sheets, sid, ITEMS_TAB, f"A{first}:P{last}") or []
    out: List[Dict[str, Any]] = []
    for row in rows:
        r = [str(x).strip() for x in row] + [""] * (len(ITEMS_HEADER) - len(row))
        iid, seen, pub, scope, source, domain, title, url = (r[0], r[1], r[2], r[3], r[8], r[9],
                                                             r[12], r[13])
        if not iid or not scope or not title or source not in allowed:
            continue
        if scope != "MACRO" and names is not None and scope.upper() not in names:
            continue
        published = _parse_stamp(pub, UTC)
        if published is None:
            continue
        hours = MEMORY_HOURS["MACRO" if scope == "MACRO" else "NAME"]
        if published < now - _dt.timedelta(hours=hours) or published > now + _dt.timedelta(hours=1):
            continue
        out.append({"id": iid, "source": source, "scope": scope, "title": title, "url": url,
                    "domain": domain, "published": published, "first_seen": _parse_stamp(seen, RIYADH),
                    "memory": True})
    return out


def _flags(g: Dict[str, Any]) -> List[Tuple[str, Dict[str, str]]]:
    return sorted(((s, f) for s, fl in g["names"].items() for f in fl),
                  key=lambda x: (_LEVEL_ORDER.get(x[1]["level"], 9), x[0]))


def econ_row(e: Dict[str, Any], now: _dt.datetime) -> List[Any]:
    def v(x: Any) -> Any:
        return "" if x is None else x
    return [e["key"], fmt_utc(e["date"]), e["country"], e["type"], e["comparison"], e["period"],
            e["importance"], v(e["estimate"]), v(e["actual"]), v(e["previous"]),
            v(e["surprise"]), v(e["surprise_pct"]), fmt_riyadh(now)]


def gate_row(rep: Dict[str, Any]) -> List[Any]:
    g = rep["gate"]
    names = "; ".join(f"{s} {f['level']}: {f['why']}" for s, f in _flags(g))
    sched = "; ".join(f"{s['event']} {fmt_riyadh(s['when'])[5:16]}" for s in g["scheduled"])
    mk = g["market"]
    market = ", ".join(x for x in [
        f"SPX {mk['spx_change_p']:+.2f}%" if mk["spx_change_p"] is not None else "",
        f"VIX {mk['vix']:.1f}" if mk["vix"] is not None else ""] if x)
    c = rep["counts"]
    return [fmt_riyadh(rep["now"]), rep["run_id"], g["verdict"], " | ".join(g["drivers"])[:1500],
            sched[:500], market, names[:1500],
            f"{c['new']}/{c['classified']}/{c['collected']}"
            + (f" +{c['carried']} carried" if c.get("carried") else ""),
            ", ".join(f"{k}={v}" for k, v in sorted(rep["sources_ok"].items())),
            # v1.5.0: 1000 characters (was 500), so the feeds a new source could not read stay named
            "; ".join(f"{k}: {v}" for k, v in sorted(rep["sources_failed"].items()))[:1000],
            rep["mode"], __version__]


def render_digest(rep: Dict[str, Any], public: bool = False) -> str:
    """The run's digest. public (v1.8.1): the flagged names and the name clusters as counts, every scope
    symbol, ticker and name masked - for the public log, step summary and evidence."""
    g = rep["gate"]
    lines = [f"## News & macro gate v{__version__} - {fmt_riyadh(rep['now'])} Riyadh",
             "", f"**Verdict: {g['verdict']}** (observe-only; mode {rep['mode']}, "
             f"{'written' if rep['wrote'] else 'dry run'})", ""]
    lines.append("**Drivers:** " + ("; ".join(g["drivers"]) if g["drivers"] else "none"))
    if g["positives"]:
        lines.append("**Easing:** " + "; ".join(g["positives"]))
    flagged = _flags(g)
    lines.append("")
    if public and flagged:
        lines.append(f"**Names:** {len(flagged)} flag(s) on {len({s for s, _ in flagged})} name(s) - kept out "
                     "of the public log (the _News_Gate tab has them)")
    else:
        lines.append("**Names:** " + ("; ".join(f"{s} {f['level']} - {f['why']}" for s, f in flagged)
                                       if flagged else "no flags"))
    lines.append("")
    lines.append("| Cluster | Scope | Grade | Tier | Sources | Headline |")
    lines.append("|---|---|---|---|---|---|")
    shown = [c for c in rep["clusters"] if not public or c.get("scope") == "MACRO"]
    for c in shown[:15]:
        lines.append(f"| {c['event_type']}{'/' + c['anchor'] if c['anchor'] else ''} | {c['scope']} | "
                     f"{c['grade']} | {c['tier']} | {len(c['domains'])} | "
                     f"{c['headline'][:110].replace('|', '/')} |")
    hidden = len(rep["clusters"]) - len(shown)
    if public and hidden:
        lines.append("")
        lines.append(f"{hidden} cluster(s) about names kept out of the public log (the _News_Items tab has them).")
    lines.append("")
    c = rep["counts"]
    lines.append(f"Items: collected {c['collected']}, classified {c['classified']}, new {c['new']}"
                 + (f" ({c['left']} more wait for a later run, {c.get('left_names', 0)} about names)"
                    if c.get("left") else "")
                 + f", carried from earlier runs {c.get('carried', 0)}"
                 + (lambda li: (f", not about the issuer {li['unlinked']} of {li['checked']} "
                                f"({'dropped' if li['mode'] == 'on' else 'kept, observe'})"
                                + (f", issuer not checked {li['unchecked']} (no company name for "
                                   f"{li.get('unnamed', 0)} symbol(s), kept)" if li.get("unchecked") else ""))
                    if li and li.get("mode") != "off" else "")(rep.get("issuer_link")) + ". "
                 f"Sources OK: {', '.join(f'{k}={v}' for k, v in sorted(rep['sources_ok'].items())) or 'none'}. "
                 f"Failed: {'; '.join(f'{k}: {v}' for k, v in sorted(rep['sources_failed'].items())) or 'none'}.")
    lines.append(inputs_line(rep))                                        # v1.12.0 (audit A6)
    detail, ages = rep.get("sources_detail") or {}, rep.get("source_ages") or {}
    if detail or ages:      # v1.5.0 [SOURCES]
        lines.append("")
        lines.append("| Source | Dated items kept | Newest (min old) | Median (min old) | Left out: outside window / "
                     "undated / repeated | Requests failed |")
        lines.append("|---|---|---|---|---|---|")
        for src in sorted(set(detail) | set(ages)):
            d, a = detail.get(src, {}), ages.get(src, {})
            lines.append(f"| {src} | {a.get('n', 0)} | {a.get('newest_min', '-')} | {a.get('median_min', '-')} | "
                         + (f"{d.get('outside', 0)} / {d.get('undated', 0)} / {d.get('repeated', 0)}"
                            if src in NEW_SOURCES else "-")
                         + f" | {d.get('failed', 0) if src in NEW_SOURCES else '-'} |")
    if rep.get("memory_error"):                 # v1.9.0: said in the digest, not only in the log
        lines.append("")
        lines.append(f"Memory not read ({rep['memory_error']}): this verdict rests on this run's own collection, "
                     "without the items recorded by earlier runs.")
    if (rep.get("llm") or {}).get("line"):      # v1.8.0 [LLM-SHADOW]
        lines.append("")
        lines.append(rep["llm"]["line"])
    lines.append("")
    lines.append("Not a licensed financial adviser; the owner decides and executes every trade.")
    text = "\n".join(lines) + "\n"
    return mask_public(text, mask_tokens(rep.get("scope") or [])) if public else text


def inputs_line(rep: Dict[str, Any]) -> str:
    """PURE, v1.12.0 (audit A6): what the verdict was computed from - the two market quotes (used, stale or
    missing) and the news sources that answered - and a warning when GO rests on missing inputs."""
    g = rep.get("gate") or {}
    m = g.get("market") or {}
    stale = " ".join(m.get("stale") or [])
    def state(code: str, value: Any) -> str:
        return "stale" if code in stale else ("used" if value is not None else "missing")
    spx, vix = state("GSPC.INDX", m.get("spx_change_p")), state("VIX.INDX", m.get("vix"))
    news = sorted(k for k in (rep.get("sources_ok") or {}) if k not in ("market", "econ"))
    line = f"**Inputs:** S&P 500 {spx}, VIX {vix}; news sources that answered: {len(news)}"
    if stale:
        line += f" (left out as stale: {stale})"
    if g.get("verdict") == "GO" and ("used" not in (spx, vix) or not news):
        line += " - GO here rests on missing inputs, not on a calm market"
    return line


def public_report(rep: Dict[str, Any]) -> Dict[str, Any]:
    """v1.8.1: the evidence .json for a public artifact - the scope and the flagged names as counts, the
    market-wide clusters only, and every scope symbol, ticker and name masked in what is left."""
    g = rep["gate"]
    pub = {k: v for k, v in rep.items() if k not in ("scope", "gate", "clusters")}
    pub["scope"] = {"names": len(rep.get("scope") or []), "detail": "kept out of the public log"}
    pub["gate"] = {k: v for k, v in g.items() if k != "names"}
    pub["gate"]["names_flagged"] = len(_flags(g))
    pub["clusters"] = [c for c in rep["clusters"] if c.get("scope") == "MACRO"]
    pub["clusters_about_names"] = len(rep["clusters"]) - len(pub["clusters"])
    if isinstance(rep.get("issuer_link"), dict):      # its examples are name headlines: counts only
        pub["issuer_link"] = {k: v for k, v in rep["issuer_link"].items() if k != "examples"}
    # the string values only, after a round trip that turns times into text (Codex P2 on #1095: masking the
    # serialised JSON could turn a number into "[name]" and break it)
    plain = json.loads(json.dumps(pub, default=_json_default, ensure_ascii=True))
    return _mask_values(plain, mask_tokens(rep.get("scope") or []))


def run(*, http: Any, sheets: Any, sid: str, now: _dt.datetime, write: bool,
        sources: Sequence[str], scope: List[Dict[str, str]], sleep: Callable[[float], None],
        run_id: str, mode: str, tax: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
    tax = tax or load_taxonomy()
    clf = Classifier(tax)
    col = Collector(http, sleep, now)
    raw: List[Dict[str, Any]] = []
    econ: List[Dict[str, Any]] = []
    market: Dict[str, Dict[str, Any]] = {}
    symbols = [s["symbol"] for s in scope]
    def stage(src: str, n: int, t0: float) -> None:
        # v1.0.1: one progress line per source, so a live log shows where the
        # time goes (the digest only prints at the end).
        why = col.failed.get(src, "")
        _out(f"[news_gate v{__version__}] {src}: {n} items in {col.clock() - t0:.0f}s"
             + (f" | {why[:160]}" if why else ""))

    _out(f"[news_gate v{__version__}] scope {len(scope)}: "
         + (", ".join(symbols)[:300] if not _PUBLIC_MASK else "(names kept out of the public log)"))
    if "edgar" in sources:
        t0 = col.clock()
        got, sec_names = col.edgar(symbols, _env("TFB_SEC_USER_AGENT"))
        raw.extend(got)
        for s in scope:
            if not s.get("name") and s["symbol"] in sec_names:
                s["name"] = sec_names[s["symbol"]]
        stage("edgar", len(got), t0)
    gq, nq = build_queries(scope)
    if "gdelt" in sources:
        t0 = col.clock()
        got = col.gdelt(gq, _float_env("TFB_NEWS_GATE_GDELT_SPACING_S", 6.0, 0.0, 60.0))
        raw.extend(got)
        stage("gdelt", len(got), t0)
    if "google_news" in sources:
        t0 = col.clock()
        got = col.google_news(nq)
        raw.extend(got)
        stage("google_news", len(got), t0)
    if "fed_rss" in sources:
        t0 = col.clock()
        got = col.fed_rss()
        raw.extend(got)
        stage("fed_rss", len(got), t0)
    # v1.5.0 [SOURCES]
    if "bing_news" in sources:
        t0 = col.clock()
        got = col.bing_news(build_bing_queries(scope))
        raw.extend(got)
        stage("bing_news", len(got), t0)
    if "finnhub" in sources:                                       # v1.6.0 [FINNHUB]
        t0 = col.clock()
        got = col.finnhub(symbols, _env("FINNHUB_API_KEY"))
        raw.extend(got)
        stage("finnhub", len(got), t0)
    if "marketaux" in sources:                                     # v1.7.0 [MARKETAUX]
        t0 = col.clock()
        got = col.marketaux(symbols, _env("MARKETAUX_API_KEY"), _int_env("TFB_NEWS_GATE_MARKETAUX_MAX", 8, 0, 30))
        raw.extend(got)
        stage("marketaux", len(got), t0)
    if "eodhd_news" in sources:                                    # v1.13.0 [SOURCES-REVIEW]
        t0 = col.clock()
        got = col.eodhd_news(symbols, _env("EODHD_API_KEY"))
        raw.extend(got)
        stage("eodhd_news", len(got), t0)
    if "yahoo_rss" in sources:
        t0 = col.clock()
        got = col.yahoo_rss(symbols)
        raw.extend(got)
        stage("yahoo_rss", len(got), t0)
    for group in FEED_GROUPS:
        if group in sources:
            t0 = col.clock()
            got = col.feeds(group, _env("TFB_SEC_USER_AGENT"))
            raw.extend(got)
            stage(group, len(got), t0)
    key = _env("EODHD_API_KEY")
    if "econ" in sources:
        t0 = col.clock()
        econ = col.econ(key)
        stage("econ", len(econ), t0)
    if "market" in sources:
        t0 = col.clock()
        market = col.market(key)
        stage("market", len(market), t0)

    memory: List[Dict[str, Any]] = []
    memory_error = ""
    if sheets is not None and sid:
        t0 = col.clock()
        for wait in MEMORY_RETRY_S:      # v1.9.0: a busy workbook (HTTP 429, a per-minute quota) is asked again
            if wait:
                sleep(wait)
            try:
                memory = read_memory(sheets, sid, now, _int_env("TFB_NEWS_GATE_MEMORY_ROWS", 2500, 0, 20000),
                                     sources, symbols)
                memory_error = ""
                break
            except Exception as exc:  # memory is a help, never a reason to fail
                memory_error = f"{type(exc).__name__}: {str(exc)[:120]}"
                if "HTTP 429" not in str(exc):
                    break
        _out(f"[news_gate v{__version__}] memory: {len(memory)} items within 24 h / 72 h in "
             f"{col.clock() - t0:.0f}s" + (f" | {memory_error}" if memory_error else ""))

    link_mode = _issuer_link_mode()   # v1.1.0 [ISSUER-LINK]
    linked, link_info = apply_issuer_link(raw + memory, {s["symbol"]: s.get("name") or "" for s in scope}, link_mode)
    if link_mode != "off":
        _out(f"[news_gate v{__version__}] issuer link ({link_mode}): {link_info['unlinked']} of "
             f"{link_info['checked']} name items not about the issuer"
             + (" dropped" if link_mode == "on" else " (kept, observe)")
             + (f"; issuer not checked {link_info['unchecked']} (no company name for {link_info['unnamed']} "
                f"symbol(s), kept)" if link_info.get("unchecked") else ""))
    # v1.5.0 [SOURCES]: one story from one publisher is one member, whichever sources carried it
    folded, repeated = fold_new_sources(linked)
    for src, n in repeated.items():
        col._tally(src, repeated=n)
    if repeated:
        _out(f"[news_gate v{__version__}] repeated by a new source and left out: "
             + ", ".join(f"{k} {v}" for k, v in sorted(repeated.items())))
    items = classify_all(folded, clf, now)
    fresh_items = [i for i in items if not i.get("memory")]
    classified = [i for i in items if i.get("event_type") and i["event_type"] != "law_firm_noise"]
    carried = [i for i in classified if i.get("memory")]
    clusters = cluster_items(classified)
    gate = evaluate(clusters, econ, market, now)
    rep: Dict[str, Any] = {
        "version": __version__, "now": now, "run_id": run_id, "mode": mode, "wrote": False,
        "scope": scope, "gate": gate, "clusters": clusters,
        "sources_ok": dict(col.ok), "sources_failed": dict(col.failed),
        "counts": {"collected": len(fresh_items), "classified": len(classified) - len(carried),
                   "new": 0, "carried": len(carried)},
        "econ": econ,
        "issuer_link": link_info,
        "sources_detail": {k: dict(v) for k, v in sorted(col.detail.items())},      # v1.5.0 [SOURCES]
        "source_ages": source_ages(fresh_items, now),
    }
    if memory_error:
        rep["memory_error"] = memory_error
    answered = bool(col.ok)
    rc = 0 if answered else 1
    if write and sheets is not None and sid:
        try:
            max_items = _int_env("TFB_NEWS_GATE_MAX_ITEMS", 200, 1, 2000)
            ensure_tab(sheets, sid, ITEMS_TAB, ITEMS_HEADER)
            existing = {str(r[0]) for r in (read_values(sheets, sid, ITEMS_TAB, "A2:A") or []) if r}
            fresh, waiting = pick_new(classified, existing, max_items)      # v1.9.0: names first
            append_rows(sheets, sid, ITEMS_TAB, [item_row(i, run_id) for i in fresh])
            rep["counts"]["new"] = len(fresh)
            rep["counts"].update(waiting)
            if econ:
                ensure_tab(sheets, sid, ECON_TAB, ECON_HEADER)
                rows = read_values(sheets, sid, ECON_TAB, "A2:M") or []
                index = {str(r[0]): (n + 2, r) for n, r in enumerate(rows) if r}
                keep = [e for e in econ if e["importance"] == "HIGH" or e["country"] == "SA"]
                upd, new = [], []
                for e in keep:
                    row = econ_row(e, now)
                    if e["key"] in index:
                        at, old = index[e["key"]]
                        if [str(x) for x in old[7:12]] != [str(x) for x in row[7:12]]:
                            upd.append((at, row))
                    else:
                        new.append(row)
                update_rows(sheets, sid, ECON_TAB, upd)
                append_rows(sheets, sid, ECON_TAB, new)
            ensure_tab(sheets, sid, GATE_TAB, GATE_HEADER)
            append_rows(sheets, sid, GATE_TAB, [gate_row(rep)])
            rep["wrote"] = True
        except Exception as exc:
            rep["write_error"] = str(exc)[:300]
            rc = 1
    # v1.8.0 [LLM-SHADOW]: after the verdict and the gate's own rows; nothing below can change either
    rep["llm"] = llm_shadow(http=http, sheets=sheets, sid=sid, items=fresh_items, tax=tax, now=now, run_id=run_id,
                            write=bool(write and rep["wrote"]), sleep=sleep, clock=col.clock)
    if rep["llm"].get("line"):
        _out(f"[news_gate v{__version__}] " + rep["llm"]["line"])
    return rc, rep


def llm_shadow(*, http: Any, sheets: Any, sid: str, items: Sequence[Dict[str, Any]], tax: Dict[str, Any],
               now: _dt.datetime, run_id: str, write: bool, sleep: Callable[[float], None],
               clock: Callable[[], float]) -> Dict[str, Any]:
    """v1.8.0 [LLM-SHADOW]: the report of the shadow reader (scripts/news_llm_reader.py). Every fault is caught
    here: the reader labels headlines for later scoring and must never fail, slow past its own budget or
    change the run that called it."""
    if _env("TFB_NEWS_LLM", "on").strip().lower() in ("off", "0", "false", "no"):
        return {"mode": "off"}
    key = _env("GEMINI_API_KEY")
    try:
        try:
            from scripts import news_llm_reader as llm  # noqa: PLC0415
        except ImportError:  # run as a file from scripts/
            import news_llm_reader as llm  # type: ignore  # noqa: PLC0415
        if not key:
            rep: Dict[str, Any] = {"mode": "no key"}
        else:
            seen: List[str] = []
            if sheets is not None and sid:
                ids = read_values(sheets, sid, llm.TAB, "A2:A") or []
                seen = [str(r[0]) for r in ids[-8000:] if r]
            rows, rep = llm.read(post=getattr(http, "post", None), key=key, model=_env("TFB_NEWS_LLM_MODEL"),
                                 items=items, taxonomy=tax, now=now, seen=seen, run_id=run_id, sleep=sleep,
                                 clock=clock, max_items=_int_env("TFB_NEWS_LLM_MAX_ITEMS", 120, 0, 400))
            rep["wrote"] = False
            if write and rows and sheets is not None and sid:
                ensure_tab(sheets, sid, llm.TAB, llm.HEADER)
                append_rows(sheets, sid, llm.TAB, rows)
                rep["wrote"] = True
        rep["line"] = llm.digest_line(rep)
        return rep
    except Exception as exc:  # noqa: BLE001
        why = _scrub(f"{type(exc).__name__}: {exc}", key)[:120]
        return {"mode": "failed", "error": why,
                "line": f"LLM shadow reader: failed ({why}); the gate is not affected."}


def _json_default(o: Any) -> Any:
    if isinstance(o, _dt.datetime):
        return o.astimezone(UTC).isoformat()
    return str(o)


def write_evidence(rep: Dict[str, Any], directory: str, public: bool = False) -> None:
    if not directory:
        return
    os.makedirs(directory, exist_ok=True)
    slim = public_report(rep) if public else dict(rep)
    slim["clusters"] = [{k: v for k, v in c.items() if k != "members"} | {"size": len(c["members"])}
                        for c in slim["clusters"]]
    with open(os.path.join(directory, "news_gate.json"), "w", encoding="utf-8") as fh:
        json.dump(slim, fh, default=_json_default, ensure_ascii=True, indent=1)
    with open(os.path.join(directory, "news_gate.md"), "w", encoding="utf-8") as fh:
        fh.write(render_digest(rep, public=public))


def main(argv: Optional[Sequence[str]] = None, *, http: Any = None, sheets: Any = None,
         now: Optional[_dt.datetime] = None, sleep: Optional[Callable[[float], None]] = None) -> int:
    try:
        return _main(argv, http=http, sheets=sheets, now=now, sleep=sleep)
    finally:
        _PUBLIC_MASK[:] = []      # v1.8.1: the mask belongs to one run


def _main(argv: Optional[Sequence[str]] = None, *, http: Any = None, sheets: Any = None,
          now: Optional[_dt.datetime] = None, sleep: Optional[Callable[[float], None]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB news & macro gate (observe-only)")
    mode_g = ap.add_mutually_exclusive_group()
    mode_g.add_argument("--write", action="store_true", help="append to the workbook tabs")
    mode_g.add_argument("--dry-run", action="store_true", help="collect and print only (default)")
    ap.add_argument("--now", default="", help="ISO time for replays (UTC if no offset)")
    args = ap.parse_args(argv)
    mode = gate_mode()
    _PUBLIC_MASK[:] = []          # v1.8.1: set once the scope is known; nothing before it names a holding
    if mode == "off":
        _out(f"[news_gate v{__version__}] TFB_NEWS_GATE is off - nothing collected, nothing written.")
        return 0
    if args.now:
        try:
            now = _dt.datetime.fromisoformat(args.now)
        except ValueError:
            _out(f"[news_gate v{__version__}] --now is not an ISO time: {args.now!r}")
            return 2
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
    now = (now or _dt.datetime.now(UTC)).astimezone(UTC)
    sid = _env("DEFAULT_SPREADSHEET_ID")
    if args.write and not sid:
        _out(f"[news_gate v{__version__}] --write needs DEFAULT_SPREADSHEET_ID")
        return 2
    if http is None:  # pragma: no cover - live run
        http = make_http_session()
    if sheets is None and sid and _credentials_info():  # pragma: no cover - live run
        try:
            sheets = make_sheets_session()
        except Exception as exc:
            _out(f"[news_gate v{__version__}] could not open the workbook session: {type(exc).__name__}")
            sheets = None
    if args.write and sheets is None:
        # A scheduled --write without a usable Sheets session must not pass
        # as a quiet dry run (Codex P2, #859).
        _out(f"[news_gate v{__version__}] --write needs readable GOOGLE_SHEETS_CREDENTIALS - "
             "nothing collected, nothing written")
        return 2
    max_names = _int_env("TFB_NEWS_GATE_MAX_NAMES", 15, 0, 60)
    scope: List[Dict[str, str]] = []
    if sheets is not None and sid:
        try:
            scope = read_scope(sheets, sid, max_names)
        except Exception as exc:
            _out(f"[news_gate v{__version__}] scope read failed ({exc}); continuing with env symbols")
    for s in parse_symbols_env(_env("TFB_NEWS_GATE_SYMBOLS")):
        if s["symbol"] not in {x["symbol"] for x in scope}:
            scope.append(s)
    public = not log_detail()
    if public:
        _PUBLIC_MASK[:] = mask_tokens(scope) or ["\x00"]     # "\x00" never occurs: masking stays on
    sources = selected_sources(_env("TFB_NEWS_GATE_SOURCES"))
    run_id = _env("GITHUB_RUN_ID") or now.strftime("local-%Y%m%dT%H%M%S")
    rc, rep = run(http=http, sheets=sheets, sid=sid, now=now, write=bool(args.write),
                  sources=sources, scope=scope, sleep=sleep or time.sleep, run_id=run_id, mode=mode)
    digest = render_digest(rep, public=public)
    _out(digest)
    write_evidence(rep, _env("TFB_NEWS_GATE_EVIDENCE_DIR"), public=public)
    summary = _env("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as fh:
                fh.write(digest)
        except OSError:
            pass
    g = rep["gate"]
    _out(f"[news_gate v{__version__}] verdict={g['verdict']} drivers={len(g['drivers'])} "
         f"names_flagged={len(g['names'])} scope={len(scope)} carried={rep['counts'].get('carried', 0)} "
         f"sources_ok={sorted(rep['sources_ok'])} sources_failed={sorted(rep['sources_failed'])} "
         f"wrote={rep['wrote']}" + (f" write_error={rep['write_error']}" if rep.get("write_error") else ""))
    return rc


if __name__ == "__main__":
    sys.exit(main())
