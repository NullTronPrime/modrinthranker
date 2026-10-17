import argparse
import gzip
import hashlib
import json
import math
import os
import random
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CACHE = DATA / "cache"
PUB = Path(os.environ.get("MR_PUB", str(ROOT / "docs")))
HIST = Path(os.environ.get("MR_HIST", str(ROOT / "history")))
for _d in (DATA, CACHE, PUB, HIST):
    _d.mkdir(parents=True, exist_ok=True)
DB = Path(os.environ.get("MR_DB", str(DATA / "ranks.sqlite")))

BASE = os.environ.get("MR_BASE", "https://api.modrinth.com/v2")
UA = os.environ.get(
    "MR_UA",
    "modrinthranker/1.0 (https://github.com/NullTronPrime/modrinthranker; ragav.dev@example.org)",
)
RPM = int(os.environ.get("MR_RPM", "90"))
TOP_N = int(os.environ.get("MR_TOP_N", "400"))
SEARCH_LIMIT = 100
SHARD_SIZE = 5000
MIN_PUBLISH = 1000
BULK_LIMIT = 800
RETRIES = 8
TTL = 24 * 3600
CACHE_ENABLED = int(os.environ.get("MR_SMOKE", "0")) == 0
HOURLY_MODE = False


def log(*a):
    print(*a, flush=True)


class PacedClient:
    def __init__(self):
        self.gap = 60.0 / RPM
        self.last = 0.0
        self.saved = 0

    def _pace(self):
        dt = time.time() - self.last
        want = self.gap * random.uniform(0.85, 1.2)
        if dt < want:
            time.sleep(want - dt)
        self.last = time.time()

    def get(self, path):
        key = re.sub(r"[^A-Za-z0-9_.-]", "_", path)
        fname = CACHE / (key[:150] + ".json")
        if CACHE_ENABLED and fname.exists() and time.time() - fname.stat().st_mtime < TTL:
            self.saved += 1
            return json.loads(fname.read_text("utf-8"))
        url = BASE + path
        req = urllib.request.Request(
            url, headers={"User-Agent": UA, "Accept": "application/json"}
        )
        for attempt in range(RETRIES):
            self._pace()
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    text = r.read().decode("utf-8")
                    if CACHE_ENABLED:
                        fname.write_text(text, "utf-8")
                    return json.loads(text)
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504):
                    reset = 30
                    if e.headers and e.headers.get("X-Ratelimit-Reset"):
                        try:
                            reset = min(int(e.headers["X-Ratelimit-Reset"]), 120)
                        except (TypeError, ValueError):
                            reset = 30
                    log("  backoff %ss %s" % (reset, path[:70]))
                    time.sleep(reset + random.uniform(0, 2))
                    continue
                log("  giveup %s %s" % (e.code, path[:70]))
                return None
            except (TimeoutError, OSError) as e:
                wait = min(5 * (attempt + 1), 40)
                log("  neterr %s retry %ss %s" % (type(e).__name__, wait, path[:60]))
                time.sleep(wait)
                continue
        raise RuntimeError("gave up on %s" % path[:90])


SCHEMA = """
CREATE TABLE IF NOT EXISTS projects(
  id TEXT PRIMARY KEY, slug TEXT, title TEXT, project_type TEXT,
  categories TEXT, additional_categories TEXT,
  client_side TEXT, server_side TEXT,
  downloads INTEGER DEFAULT 0, followers INTEGER DEFAULT 0,
  versions INTEGER DEFAULT 0, status TEXT, published TEXT, updated TEXT,
  team TEXT, org TEXT, author TEXT, author_id TEXT,
  icon_url TEXT, color INTEGER, featured INTEGER, thread_id TEXT,
  license TEXT, issues_url TEXT, source_url TEXT, wiki_url TEXT,
  discord_url TEXT, gallery INTEGER DEFAULT 0,
  last_checked INTEGER);
CREATE TABLE IF NOT EXISTS project_team(
  project_id TEXT, user_id TEXT, username TEXT, name TEXT,
  is_owner INTEGER, role TEXT,
  PRIMARY KEY(project_id, user_id));
CREATE TABLE IF NOT EXISTS deps(
  project_id TEXT, dep_project_id TEXT, dependency_type TEXT,
  PRIMARY KEY(project_id, dep_project_id));
CREATE TABLE IF NOT EXISTS users(
  id TEXT PRIMARY KEY, username TEXT, name TEXT, bio TEXT,
  followers INTEGER, joined TEXT, role TEXT, projects_created INTEGER,
  last_checked INTEGER, avatar_url TEXT, badges INTEGER, github_id TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS team_projects(
  team_id TEXT, project_id TEXT,
  PRIMARY KEY(team_id, project_id));
CREATE TABLE IF NOT EXISTS changelog(
  id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, kind TEXT);
CREATE INDEX IF NOT EXISTS ix_dl ON projects(downloads DESC);
CREATE INDEX IF NOT EXISTS ix_pt_user ON project_team(user_id);
CREATE INDEX IF NOT EXISTS ix_pt_proj ON project_team(project_id);
CREATE INDEX IF NOT EXISTS ix_tp_team ON team_projects(team_id);
CREATE INDEX IF NOT EXISTS ix_tp_proj ON team_projects(project_id);
"""


PROJECT_ADDITIONS = [
    ("issues_url", "ALTER TABLE projects ADD COLUMN issues_url TEXT"),
    ("source_url", "ALTER TABLE projects ADD COLUMN source_url TEXT"),
    ("wiki_url", "ALTER TABLE projects ADD COLUMN wiki_url TEXT"),
    ("discord_url", "ALTER TABLE projects ADD COLUMN discord_url TEXT"),
    ("gallery", "ALTER TABLE projects ADD COLUMN gallery INTEGER DEFAULT 0"),
    ("client_side", "ALTER TABLE projects ADD COLUMN client_side TEXT"),
    ("server_side", "ALTER TABLE projects ADD COLUMN server_side TEXT"),
    ("additional_categories", "ALTER TABLE projects ADD COLUMN additional_categories TEXT"),
    ("versions", "ALTER TABLE projects ADD COLUMN versions INTEGER DEFAULT 0"),
]


USER_ADDITIONS = [
    ("avatar_url", "ALTER TABLE users ADD COLUMN avatar_url TEXT"),
    ("badges", "ALTER TABLE users ADD COLUMN badges INTEGER"),
    ("github_id", "ALTER TABLE users ADD COLUMN github_id TEXT"),
]


def init_db(c):
    c.executescript(SCHEMA)
    cols = {r[1] for r in c.execute("PRAGMA table_info(projects)")}
    for name, ddl in PROJECT_ADDITIONS:
        if name not in cols:
            c.execute(ddl)
    ucols = {r[1] for r in c.execute("PRAGMA table_info(users)")}
    for name, ddl in USER_ADDITIONS:
        if name not in ucols:
            c.execute(ddl)
    c.commit()


def jdump(v):
    try:
        return json.dumps(v, separators=(",", ":"))
    except (TypeError, ValueError):
        return "[]"


def jload(v, default):
    try:
        x = json.loads(v)
        return x if x is not None else default
    except (TypeError, ValueError):
        return default


def upsert_project(c, p, checked):
    lic = p.get("license") or {}
    license_id = lic.get("id") if isinstance(lic, dict) else lic
    new = (
        p.get("downloads", 0) or 0,
        p.get("followers", 0) or 0,
        _count_versions(p),
    )
    old = None
    if HOURLY_MODE:
        old = c.execute(
            "SELECT downloads, followers, versions FROM projects WHERE id=?", (p["id"],)
        ).fetchone()
    c.execute(
        """INSERT INTO projects VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
          downloads=excluded.downloads, followers=excluded.followers,
          versions=excluded.versions, updated=excluded.updated,
          status=excluded.status, categories=excluded.categories,
          additional_categories=excluded.additional_categories,
          license=excluded.license, last_checked=excluded.last_checked""",
        (
            p["id"], p.get("slug"), p.get("title"), p.get("project_type"),
            jdump(p.get("categories")), jdump(p.get("additional_categories")),
            jdump(p.get("client_side")), jdump(p.get("server_side")),
            new[0], new[1], new[2],
            p.get("status"), p.get("published"), p.get("updated"),
            p.get("team"), p.get("organization"), p.get("author"), p.get("author_id"),
            p.get("icon_url"), p.get("color"), 1 if p.get("featured") else 0,
            p.get("thread_id"), license_id, p.get("issues_url"), p.get("source_url"),
            p.get("wiki_url"), p.get("discord_url"), len(p.get("gallery") or []),
            checked,
        ),
    )
    if HOURLY_MODE:
        if old is None:
            c.execute("INSERT OR REPLACE INTO changelog VALUES(?,?,?,?,?)",
                      (p["id"], new[0], new[1], new[2], "new"))
        elif (old[0] or 0, old[1] or 0, old[2] or 0) != new:
            c.execute("INSERT OR REPLACE INTO changelog VALUES(?,?,?,?,?)",
                      (p["id"], new[0] - (old[0] or 0), new[1] - (old[1] or 0),
                       new[2] - (old[2] or 0), "changed"))


def ingest_ids(client, c, ids, checked):
    if not ids:
        return
    projs = client.get("/projects?ids=" + urllib.parse.quote(json.dumps(ids)))
    if not projs:
        return
    for p in projs:
        upsert_project(c, p, checked)


def walk(client, c, index):
    offset = 0
    total = None
    seen = 0
    while True:
        q = "/search?index=%s&limit=%d&offset=%d" % (index, SEARCH_LIMIT, offset)
        body = client.get(q)
        if not body or not body.get("hits"):
            break
        total = body.get("total_hits") or total or 0
        hits = body["hits"]
        ingest_ids(client, c, [h.get("project_id") for h in hits if h.get("project_id")], int(time.time()))
        seen += len(hits)
        offset += SEARCH_LIMIT
        if offset % 1000 == 0:
            log("  %s offset %d/%s seen %d" % (index, offset, total, seen))
            c.commit()
        if offset >= total:
            break
    return seen, total or 0


def link_teams(c):
    c.execute(
        """INSERT OR IGNORE INTO team_projects(team_id, project_id)
           SELECT team, id FROM projects WHERE team IS NOT NULL AND team <> ''"""
    )
    c.commit()
    return c.execute("SELECT COUNT(*) FROM team_projects").fetchone()[0]


def collect_teams(client, c, team_ids):
    tids = sorted(set(t for t in team_ids if t))
    members = 0
    for i in range(0, len(tids), BULK_LIMIT):
        chunk = tids[i : i + BULK_LIMIT]
        body = client.get("/teams?ids=" + urllib.parse.quote(json.dumps(chunk)))
        if not body:
            continue
        for team_members in body:
            if not team_members:
                continue
            m0 = team_members[0]
            tid = m0.get("team_id")
            for m in team_members:
                u = m.get("user") or {}
                c.execute(
                    "INSERT OR REPLACE INTO project_team VALUES(?,?,?,?,?,?)",
                    (tid, u.get("id") or m.get("user_id"), u.get("username"),
                     m.get("name"), 1 if m.get("is_owner") else 0, m.get("role")),
                )
                members += 1
        if (i // BULK_LIMIT) % 10 == 0:
            log("  teams %d/%d members %d" % (i + len(chunk), len(tids), members))
    return members


def collect_users(client, c, user_ids):
    uids = sorted(set(u for u in user_ids if u))
    got = 0
    for i in range(0, len(uids), BULK_LIMIT):
        chunk = uids[i : i + BULK_LIMIT]
        body = client.get("/users?ids=" + urllib.parse.quote(json.dumps(chunk)))
        if not body:
            continue
        checked = int(time.time())
        for u in body:
            c.execute(
                """INSERT OR REPLACE INTO users
                   (id, username, name, bio, followers, joined, role,
                    projects_created, last_checked, avatar_url, badges, github_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (u.get("id"), u.get("username"), u.get("name"), u.get("bio"),
                 u.get("followers", 0) or 0, u.get("created"), u.get("role"),
                 u.get("projects_created", 0) or 0, checked, u.get("avatar_url"),
                 u.get("badges", 0) or 0, u.get("github_id")),
            )
            got += 1
    return got


def crawl_deps(client, c, top_k, ids=None):
    if ids is None:
        rows = c.execute(
            "SELECT id FROM projects ORDER BY downloads DESC LIMIT ?", (top_k,)
        ).fetchall()
    else:
        rows = [(i,) for i in ids]
    edges = 0
    for n, (pid,) in enumerate(rows, 1):
        d = client.get("/project/%s/dependencies" % pid)
        if d:
            for pr in d.get("projects") or []:
                if pr.get("id"):
                    c.execute("INSERT OR IGNORE INTO deps VALUES(?,?,?)",
                              (pid, pr["id"], pr.get("dependency_type")))
                    edges += 1
            for v in d.get("versions") or []:
                for dep in v.get("dependencies") or []:
                    target = dep.get("project_id")
                    if target:
                        c.execute("INSERT OR IGNORE INTO deps VALUES(?,?,?)",
                                  (pid, target, dep.get("dependency_type")))
                        edges += 1
        if n % 50 == 0:
            c.commit()
            log("  deps %d/%d edges %d" % (n, len(rows), edges))
    return edges


def build_graph(c, projects, inbound, limit=None):
    if limit is None:
        limit = int(os.environ.get("MR_GRAPH_LIMIT", "500"))
    sel = projects[:limit]
    ids = {p["id"] for p in sel}
    nodes = []
    seen = set()
    for p in sel:
        key = "p:" + p["id"]
        if key in seen:
            continue
        seen.add(key)
        nodes.append({
            "id": key, "label": p["title"] or p["id"], "type": "project",
            "slug": p.get("slug"), "downloads": p.get("downloads") or 0,
            "depended_by": inbound.get(p["id"], 0), "icon": p.get("icon_url"),
        })
    edges = []
    members = {}
    avatars = {}
    try:
        for uid, av in c.execute("SELECT id, avatar_url FROM users WHERE avatar_url IS NOT NULL"):
            avatars[uid] = av
    except sqlite3.Error:
        pass
    for pid, uid, uname in c.execute(
        """SELECT tp.project_id, pt.user_id, pt.username
           FROM team_projects tp JOIN project_team pt ON pt.project_id = tp.team_id
           WHERE tp.project_id IN (%s)""" % ",".join("?" * len(ids)),
        tuple(ids),
    ):
        members.setdefault(pid, []).append((uid, uname))
    for pid, people in members.items():
        for uid, uname in people:
            key = "u:" + uid
            if key not in seen:
                seen.add(key)
                nodes.append({
                "id": key, "label": uname or uid, "type": "user",
                "avatar": avatars.get(uid),
            })
            edges.append({"source": key, "target": "p:" + pid, "kind": "works"})
        for i in range(len(people)):
            for j in range(i + 1, len(people)):
                edges.append({
                    "source": "u:" + people[i][0], "target": "u:" + people[j][0],
                    "kind": "collab",
                })
    for src, tgt, kind in c.execute("SELECT project_id, dep_project_id, dependency_type FROM deps"):
        if src in ids and tgt in ids:
            edges.append({"source": "p:" + src, "target": "p:" + tgt, "kind": "depends", "type": kind})
    return {"nodes": nodes, "edges": edges}


CURVE_POINTS = int(os.environ.get("MR_CURVE_POINTS", "1200"))
TREND_TRACK = int(os.environ.get("MR_TREND_TRACK", "2000"))
TREND_DAYS = int(os.environ.get("MR_TREND_DAYS", "365"))


def _count_versions(p):
    v = p.get("versions")
    if isinstance(v, list):
        return len(v)
    if isinstance(v, int):
        return v
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _xml_esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def emit_download_curve(c):
    """Rank-vs-downloads curve over ALL projects.

    The heavy work (sorting ~all mods) happens here once at publish time.
    The frontend only displays the pre-rendered SVG image + a small
    downsampled JSON, so no per-visit calculation is needed.
    """
    rows = [r[0] or 0 for r in c.execute(
        "SELECT downloads FROM projects ORDER BY downloads DESC")]
    n = len(rows)
    if not n:
        return {"count": 0}
    total = sum(rows)
    srt = sorted(rows)
    median = srt[n // 2]
    mean = total / n
    top_rows = c.execute(
        "SELECT id, title, slug, downloads FROM projects "
        "ORDER BY downloads DESC LIMIT 10").fetchall()
    top = [{"id": r[0], "title": r[1], "slug": r[2], "downloads": r[3] or 0}
           for r in top_rows]
    pts = CURVE_POINTS
    if n <= pts:
        sampled = [[i + 1, rows[i]] for i in range(n)]
    else:
        sampled = [[int(round(i * (n - 1) / (pts - 1))) + 1,
                    rows[int(round(i * (n - 1) / (pts - 1)))]]
                   for i in range(pts)]
    payload = {
        "generated": int(time.time()),
        "count": n,
        "max": rows[0],
        "min": rows[-1],
        "median": median,
        "mean": round(mean, 1),
        "total": total,
        "points": sampled,
        "top": top,
    }
    write_if_changed(PUB / "download-curve.json", json.dumps(payload))
    # Pre-rendered SVG image (log-scale y). Static file: no client compute.
    W, H = 1160, 440
    ML, MR, MT, MB = 76, 18, 30, 46
    PW, PH = W - ML - MR, H - MT - MB
    import math as _math
    ymax = max(10, rows[0])
    ytop = 10 ** _math.ceil(_math.log10(ymax))
    ymin = 1
    lo, hi = _math.log10(ymin), _math.log10(ytop)

    def X(rank):
        return ML + (rank - 1) / max(n - 1, 1) * PW

    def Y(dl):
        v = max(dl, ymin)
        return MT + PH - (_math.log10(v) - lo) / (hi - lo) * PH

    parts = []
    parts.append(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" '
        'role="img" aria-label="downloads by rank">' % (W, H))
    parts.append('<rect width="%d" height="%d" fill="#1a1a1d"/>' % (W, H))
    # gridlines: powers of 10
    p = 1
    while p <= ytop:
        y = Y(p)
        parts.append(
            '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#2b2b30" '
            'stroke-width="1"/>' % (ML, y, W - MR, y))
        label = "1" if p == 1 else ("1M" if p == 1000000 else
               ("%dM" % (p // 1000000) if p >= 1000000 else
                ("%dk" % (p // 1000) if p >= 1000 else str(p))))
        parts.append(
            '<text x="%d" y="%.1f" fill="#8a8a93" font-size="12" '
            'text-anchor="end" dominant-baseline="middle">%s</text>'
            % (ML - 8, y, label))
        p *= 10
    # bars (downsampled ranks, computed from all n mods)
    bw = max(1.0, PW / len(sampled) - 0.4)
    for rank, dl in sampled:
        x = X(rank)
        y = Y(dl)
        parts.append(
            '<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" '
            'fill="#1bd96a" opacity="0.55"><title>rank %s: %s downloads'
            '</title></rect>'
            % (x - bw / 2, y, bw, MT + PH - y,
               _xml_esc(format(rank, ",d")),
               _xml_esc(format(dl, ",d"))))
    # spine line through sampled points
    d = "M" + " L".join("%.1f %.1f" % (X(r), Y(v)) for r, v in sampled)
    parts.append('<path d="%s" fill="none" stroke="#1bd96a" stroke-width="1.6" '
                 'stroke-linejoin="round"/>' % d)
    # x ticks
    cand = [1, 10, 100, 1000, 10000, 25000, 50000, 100000, 150000, 200000]
    for t in cand:
        if t > n:
            break
        x = X(t)
        parts.append(
            '<line x1="%.1f" y1="%d" x2="%.1f" y2="%d" stroke="#2b2b30"/>'
            % (x, MT + PH, x, MT + PH + 5))
        parts.append(
            '<text x="%.1f" y="%d" fill="#8a8a93" font-size="12" '
            'text-anchor="middle">%s</text>'
            % (x, MT + PH + 20, format(t, ",d")))
    parts.append(
        '<text x="%d" y="%d" fill="#e0e0e4" font-size="14" font-weight="600">'
        'Downloads by rank &mdash; all %s projects (pre-rendered, log scale)'
        '</text>' % (ML, 20, _xml_esc(format(n, ",d"))))
    parts.append(
        '<text x="%d" y="%d" fill="#8a8a93" font-size="12">rank 1 (most '
        'downloaded) on the left &middot; median %s &middot; max %s</text>'
        % (ML, H - 8, _xml_esc(format(median, ",d")),
            _xml_esc(format(rows[0], ",d"))))
    parts.append('</svg>')
    write_if_changed(PUB / "download-curve.svg", "".join(parts))
    return {"count": n, "max": rows[0], "median": median, "total": total}


def _load_series():
    try:
        data = json.loads((HIST / "mod-series.json").read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_series(series):
    HIST.mkdir(exist_ok=True)
    tmp = HIST / "mod-series.json.tmp"
    tmp.write_text(json.dumps(series, separators=(",", ":")), "utf-8")
    tmp.replace(HIST / "mod-series.json")


def record_mod_series(c, today=None, cur=None):
    """Append today's downloads for the top tracked mods.

    Idempotent for a given date. This is what powers the per-mod
    "downloads per day" trend view; history accumulates with each snapshot.
    """
    today = today or time.strftime("%Y-%m-%d", time.gmtime())
    if cur is None:
        cur = {}
        for i, d, t, s in c.execute(
                "SELECT id, downloads, title, slug FROM projects"):
            cur[i] = (d or 0, t, s)
    else:
        # lean_map rows are (dl, f, v, title, slug, published, status, updated)
        cur = {i: (v[0], v[3], v[4]) for i, v in cur.items()}
    ranked = sorted(cur.items(), key=lambda kv: -kv[1][0])
    tracked = {i for i, _ in ranked[:TREND_TRACK]}
    series = _load_series()
    backfilled = False
    if not series:
        # Backfill from existing snapshot history so the trend view has
        # something on day one (usually a single point).
        try:
            index = load_json(HIST / "index.json") or {}
            for day in (index.get("days") or [])[-TREND_DAYS:]:
                d = day.get("date")
                if not d:
                    continue
                st, _ = state_at(d)
                if not st:
                    continue
                for i, _ in ranked[:TREND_TRACK]:
                    # cumulative downloads never fall, so a historical value
                    # above today's means the database was rebuilt from a
                    # different crawl: drop it instead of faking a huge drop
                    if i in st and st[i][0] <= cur[i][0]:
                        lst = series.setdefault(i, [])
                        if not lst or lst[-1][0] != d:
                            lst.append([d, st[i][0]])
            backfilled = True
        except (OSError, ValueError):
            pass
    for i in tracked:
        dl = cur[i][0]
        lst = series.setdefault(i, [])
        if lst and lst[-1][0] == today:
            lst[-1][1] = dl
        elif lst and lst[-1][1] > dl:
            # cumulative downloads only grow; a lower reading means the
            # census database was rebuilt from a different crawl
            lst = series[i] = []
            lst.append([today, dl])
        else:
            lst.append([today, dl])
        del lst[:-TREND_DAYS]
    if len(series) > TREND_TRACK * 2:
        keep = tracked + set()
        for i in list(series.keys()):
            if i not in tracked and i not in cur:
                del series[i]
            if len(series) <= TREND_TRACK * 2:
                break
    _save_series(series)
    return {"tracked": len(tracked), "stored": len(series),
            "backfilled": backfilled}


def emit_mod_trends(c, today=None):
    today = today or time.strftime("%Y-%m-%d", time.gmtime())
    info = record_mod_series(c, today)
    series = _load_series()
    meta_rows = {}
    for i, t, s, d in c.execute(
            "SELECT id, title, slug, downloads FROM projects"):
        meta_rows[i] = (t, s, d or 0)
    mods = []
    for i, lst in series.items():
        m = meta_rows.get(i)
        if not m:
            continue
        lst = lst[-TREND_DAYS:]
        first = lst[0][1] if lst else 0
        last = lst[-1][1] if lst else 0
        span = max(len(lst) - 1, 1)
        mods.append({
            "id": i, "title": m[0], "slug": m[1], "downloads": m[2],
            "points": len(lst),
            "first": first, "last": last,
            "gained": last - first,
            "per_day": round((last - first) / span, 1),
            "series": lst,
        })
    mods.sort(key=lambda m: -m["downloads"])
    payload = {
        "generated": int(time.time()),
        "tracked": info.get("tracked", 0),
        "count": len(mods),
        "days": TREND_DAYS,
        "mods": mods[:TREND_TRACK],
    }
    write_gz(PUB / "mod-trends.json.gz", payload)
    (PUB / "mod-trends.json").unlink(missing_ok=True)
    return {"count": len(mods)}


def emit(client, c, light=False):
    orgmap = org_labels(c)
    projects = []
    for r in c.execute(
        """SELECT id, slug, title, project_type, categories, additional_categories,
          client_side, server_side, downloads, followers, versions, status,
          published, updated, org, author, author_id, icon_url, color, featured,
          license, issues_url, source_url, wiki_url, discord_url, gallery
          FROM projects ORDER BY downloads DESC""" + (" LIMIT 250" if light else "")
    ):
        members = [
            m[0]
            for m in c.execute(
                """SELECT pt.username FROM team_projects tp
                  JOIN project_team pt ON pt.project_id = tp.team_id
                  WHERE tp.project_id = ?
                  ORDER BY pt.is_owner DESC, pt.username""",
                (r[0],),
            )
        ]
        projects.append({
            "id": r[0], "slug": r[1], "title": r[2], "project_type": r[3],
            "categories": jload(r[4], []), "additional_categories": jload(r[5], []),
            "client_side": jload(r[6], []), "server_side": jload(r[7], []),
            "downloads": r[8], "followers": r[9], "versions": r[10],
            "published": r[12], "updated": r[13],
            "org": r[14], "org_name": (orgmap.get(r[14]) if r[14] else None),
            "author": r[15], "author_id": r[16],
            "members": members, "contributors": max(len(members) - 1, 0),
            "icon_url": r[17], "color": r[18], "featured": r[19],
            "license": r[20], "issues_url": r[21], "source_url": r[22],
            "wiki_url": r[23], "discord_url": r[24], "gallery": r[25],
        })

    dl_rows = c.execute(
        """SELECT pt.user_id, SUM(p.downloads), COUNT(DISTINCT p.id),
             MAX(u.username), MAX(u.name), MAX(u.followers), MAX(u.joined),
             MAX(u.avatar_url), MAX(u.badges)
          FROM team_projects tp
          JOIN project_team pt ON pt.project_id = tp.team_id
          JOIN projects p ON p.id = tp.project_id
          LEFT JOIN users u ON u.id = pt.user_id
          GROUP BY pt.user_id"""
    ).fetchall()

    authors = []
    for uid, total_dl, np_, uname, nm, followers, joined, avatar, badges in dl_rows:
        if not np_:
            continue
        authors.append({
            "id": uid, "username": uname, "name": nm,
            "downloads": total_dl or 0, "projects": np_,
            "downloads_per_project": round((total_dl or 0) / np_, 1),
            "followers": followers or 0, "joined": joined,
            "avatar_url": avatar, "badges": badges or 0,
        })
    authors.sort(key=lambda a: a["downloads"], reverse=True)

    collab = {}
    for a, b, n in c.execute(
        """SELECT a.user_id, b.user_id, COUNT(DISTINCT tp.project_id)
          FROM team_projects tp
          JOIN project_team a ON a.project_id = tp.team_id
          JOIN project_team b ON b.project_id = tp.team_id AND a.user_id <> b.user_id
          GROUP BY a.user_id, b.user_id"""
    ):
        collab.setdefault(a, []).append({"with": b, "projects": n})
    for a in authors:
        partners = collab.get(a["id"], [])
        partners.sort(key=lambda x: -x["projects"])
        a["collaborators"] = partners[:50]
        a["collaborator_count"] = len(partners)

    teams = []
    for tid, members, nproj in c.execute(
        """SELECT pt.project_id, COUNT(pt.user_id), COUNT(DISTINCT tp.project_id)
          FROM project_team pt
          LEFT JOIN team_projects tp ON tp.team_id = pt.project_id
          GROUP BY pt.project_id"""
    ):
        teams.append({"team": tid, "members": members, "projects": nproj})

    deps = [
        {"from": a, "to": b, "type": t}
        for a, b, t in c.execute("SELECT project_id, dep_project_id, dependency_type FROM deps")
    ]

    cat_counts = {}
    type_counts = {}
    for r in c.execute("SELECT categories, project_type FROM projects"):
        for k in jload(r[0], []):
            cat_counts[k] = cat_counts.get(k, 0) + 1
        if r[1]:
            type_counts[r[1]] = type_counts.get(r[1], 0) + 1

    stats = client.get("/statistics") or {}
    meta = {
        "generated": int(time.time()),
        "projects": c.execute("SELECT COUNT(*) FROM projects").fetchone()[0],
        "authors": c.execute("SELECT COUNT(DISTINCT user_id) FROM project_team").fetchone()[0],
        "users": c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "teams": len(teams),
        "deps": len(deps),
        "modrinth_statistics": stats,
    }

    PUB.mkdir(exist_ok=True)

    if light:
        titles = {}
        slugs = {}
        dls = {}
        for i, t, s, d in c.execute("SELECT id, title, slug, downloads FROM projects"):
            titles[i] = t
            slugs[i] = s
            dls[i] = d or 0
        inbound = {}
        for tgt, cnt in c.execute(
            "SELECT dep_project_id, COUNT(DISTINCT project_id) FROM deps GROUP BY dep_project_id"
        ):
            inbound[tgt] = cnt
        write_if_changed(PUB / "top-projects.json", json.dumps(projects[:250]))
        write_if_changed(PUB / "top-authors.json", json.dumps(authors[:250]))
        write_if_changed(
            PUB / "depended.json",
            json.dumps(
                [
                    {"id": i, "title": titles.get(i), "slug": slugs.get(i),
                     "depended_by": n, "downloads": dls.get(i, 0)}
                    for i, n in sorted(inbound.items(), key=lambda kv: (-kv[1], kv[0]))
                    if titles.get(i)
                ]
            ),
        )
        write_if_changed(PUB / "meta.json", json.dumps(meta, indent=1))
        log("emitted light %s" % json.dumps(meta))
        return meta
    for stale in PUB.glob("projects-*.json"):
        stale.unlink()
    for stale in PUB.glob("authors-*.json"):
        stale.unlink()
    for name in ("projects.json", "authors.json", "top.json"):
        old = PUB / name
        if old.exists():
            old.unlink()

    def shard_out(items, prefix, size, top_n):
        out = []
        for start in range(0, len(items), size):
            chunk = items[start : start + size]
            name = "%s-%04d.json" % (prefix, start // size)
            write_if_changed(PUB / name, json.dumps(chunk))
            out.append({"file": name, "count": len(chunk), "offset": start})
        write_if_changed(
            PUB / (prefix + "-index.json"),
            json.dumps({"count": len(items), "shard_size": size, "shards": out}),
        )
        write_if_changed(PUB / ("top-" + prefix + ".json"), json.dumps(items[:top_n]))
        return out

    shard_out(projects, "projects", SHARD_SIZE, 250)
    shard_out(authors, "authors", SHARD_SIZE, 250)
    by_id = {p["id"]: p for p in projects}
    facets = emit_facets(c, projects)
    boards = emit_facet_boards(c, by_id, facets)
    meta["facets"] = len(facets)
    meta["facet_boards"] = len(boards)
    meta["project_shards"] = math.ceil(len(projects) / SHARD_SIZE)
    (PUB / "teams.json").write_text(json.dumps(teams), "utf-8")
    titles = {p["id"]: p["title"] for p in projects}
    slugs = {p["id"]: p.get("slug") for p in projects}
    rich = [
        {
            "from": d["from"], "to": d["to"], "type": d.get("type"),
            "from_title": titles.get(d["from"]), "from_slug": slugs.get(d["from"]),
            "to_title": titles.get(d["to"]), "to_slug": slugs.get(d["to"]),
        }
        for d in deps
    ]
    (PUB / "deps.json").write_text(json.dumps(rich), "utf-8")
    inbound = {}
    for tgt, cnt in c.execute(
        "SELECT dep_project_id, COUNT(DISTINCT project_id) FROM deps GROUP BY dep_project_id"
    ):
        inbound[tgt] = cnt
    downloads = {p["id"]: p.get("downloads") or 0 for p in projects}
    depended = [
        {
            "id": i, "title": titles.get(i), "slug": slugs.get(i),
            "depended_by": n, "downloads": downloads.get(i, 0),
        }
        for i, n in sorted(inbound.items(), key=lambda kv: (-kv[1], kv[0]))
        if titles.get(i)
    ]
    (PUB / "depended.json").write_text(json.dumps(depended), "utf-8")
    meta["depended"] = len(depended)
    try:
        curve = emit_download_curve(c)
        meta["download_curve"] = curve
    except (sqlite3.Error, OSError, ValueError) as e:
        log("download curve skipped: %s" % e)
    try:
        trends = emit_mod_trends(c)
        meta["mod_trends"] = trends
    except (sqlite3.Error, OSError, ValueError) as e:
        log("mod trends skipped: %s" % e)
    (PUB / "graph.json").write_text(json.dumps(build_graph(c, projects, inbound)), "utf-8")
    meta["deps_ranked"] = sum(
        1 for d in rich
        if (titles.get(d["to"]) or "") != "" and (d.get("type") or "") != ""
    )
    meta["dep_projects_crawled"] = c.execute(
        "SELECT COUNT(DISTINCT project_id) FROM deps"
    ).fetchone()[0]
    (PUB / "categories.json").write_text(
        json.dumps({"categories": cat_counts, "types": type_counts}), "utf-8"
    )
    (PUB / "meta.json").write_text(json.dumps(meta, indent=1), "utf-8")
    return meta


def get_meta(c, key, default=None):
    row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(c, key, value):
    c.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(value)))


def hourly(client, c):
    global HOURLY_MODE
    HOURLY_MODE = True
    checked = int(time.time())
    top_n = int(os.environ.get("MR_TOP_N", "5000"))
    slices = int(os.environ.get("MR_SLICES", "24"))
    hit = client.get("/search?index=downloads&limit=%d&offset=0" % SEARCH_LIMIT)
    ids = [h.get("project_id") for h in (hit or {}).get("hits", []) if h.get("project_id")]
    offset = SEARCH_LIMIT
    while len(ids) < top_n:
        page = client.get("/search?index=downloads&limit=%d&offset=%d" % (SEARCH_LIMIT, offset))
        batch = [h.get("project_id") for h in (page or {}).get("hits", []) if h.get("project_id")]
        if not batch:
            break
        ids.extend(batch)
        offset += SEARCH_LIMIT
    top_ids = ids[:top_n]
    log("hourly top %d" % len(top_ids))
    ingest_ids(client, c, top_ids, checked)
    c.commit()
    slice_no = int(get_meta(c, "slice_no", "0") or 0)
    rows = c.execute(
        "SELECT id FROM projects WHERE id NOT IN (SELECT value FROM json_each(?)) ORDER BY downloads DESC",
        (json.dumps(top_ids),),
    ).fetchall()
    per = max(1, len(rows) // slices)
    start = slice_no * per
    chunk = [r[0] for r in rows[start : start + per]]
    log("slice %d/%d %d projects" % (slice_no, slices, len(chunk)))
    ingest_ids(client, c, chunk, checked)
    teams = c.execute(
        """SELECT DISTINCT team FROM projects
           WHERE last_checked >= ? AND team IS NOT NULL""", (checked,)
    ).fetchall()
    fresh = [r[0] for r in teams]
    if fresh:
        collect_teams(client, c, fresh)
    link_teams(c)
    dep_top = int(os.environ.get("MR_DEP_TOP", "20000"))
    dep_per = max(1, dep_top // slices)
    dep_slice = [r[0] for r in c.execute(
        "SELECT id FROM projects ORDER BY downloads DESC LIMIT ? OFFSET ?",
        (dep_per, (slice_no % max(1, dep_top // dep_per)) * dep_per),
    )]
    edges = crawl_deps(client, c, len(dep_slice), dep_slice)
    log("slice deps %d edges %d" % (len(dep_slice), edges))
    set_meta(c, "slice_no", (slice_no + 1) % slices)
    set_meta(c, "last_hourly", checked)
    c.commit()
    stats = emit_hourly(c)
    return {"top": len(top_ids), "slice": len(chunk), "deps": edges, "diff": stats}


def write_if_changed(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, "utf-8")
    if path.exists() and path.stat().st_size == tmp.stat().st_size:
        if path.read_bytes() == tmp.read_bytes():
            tmp.unlink()
            return False
    tmp.replace(path)
    return True


def load_json(path):
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        return None


def write_gz(path, obj):
    data = gzip.compress(json.dumps(obj, separators=(",", ":")).encode("utf-8"), 9)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    if path.exists() and path.read_bytes() == data:
        tmp.unlink()
        return False
    tmp.replace(path)
    return True


def read_hours():
    h = load_json(HIST / "hours.json")
    return h if isinstance(h, list) else []


def write_census(c, ts):
    ids, dl, f, v = [], [], [], []
    for i, d, fo, ve in c.execute(
        "SELECT id, downloads, followers, versions FROM projects ORDER BY id"
    ):
        ids.append(i)
        dl.append(d or 0)
        f.append(fo or 0)
        v.append(ve or 0)
    return write_gz(PUB / "census.json.gz",
                    {"v": 1, "t": ts, "n": len(ids), "ids": ids, "dl": dl, "f": f, "v": v})


def lean_map(c):
    rows = {}
    for i, d, f, v, t, s, p, st, up in c.execute(
        "SELECT id, downloads, followers, versions, title, slug, published, status, updated FROM projects"
    ):
        rows[i] = (d or 0, f or 0, v or 0, t, s, p, st or "", up or "")
    return rows


def lean_rows(cur):
    rows = [[i, v[0], v[1], v[2]] for i, v in cur.items()]
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows


def state_at(upto):
    fulls = sorted(HIST.glob("full-*.json"))
    deltas = sorted((HIST / "deltas").glob("delta-*.json")) if (HIST / "deltas").exists() else []
    base = None
    for f in fulls:
        if f.stem[len("full-"):] <= upto:
            base = f
    if base is None and fulls:
        base = fulls[0]
    if base is None:
        return None, None
    data = load_json(base) or {"rows": []}
    st = {r[0]: [r[1], r[2], r[3]] for r in data["rows"]}
    base_date = base.stem[len("full-"):]
    last = base_date
    for f in deltas:
        d = f.stem[len("delta-"):]
        if d > upto:
            break
        if d <= base_date:
            continue
        part = load_json(f) or {}
        for r in part.get("new", []):
            st[r[0]] = [r[1], r[2], r[3]]
        for r in part.get("changed", []):
            st[r[0]] = [r[-3], r[-2], r[-1]]
        for i in part.get("removed", []):
            st.pop(i, None)
        last = d
    return st, last


def emit_analytics(index, cur):
    days = index.get("days", [])
    series = []
    for idx, d in enumerate(days):
        prev = days[idx - 1] if idx > 0 else None
        dl = d.get("downloads", 0) or 0
        pv = (prev.get("downloads", 0) or 0) if prev else dl
        pr = (prev.get("projects", 0) or 0) if prev else d.get("projects", 0) or 0
        series.append({
            "date": d["date"], "projects": d.get("projects", 0) or 0,
            "downloads": dl, "new": d.get("new", 0) or 0,
            "removed": d.get("removed", 0) or 0,
            "changed": d.get("changed", 0) or 0,
            "followers": d.get("followers", 0) or 0,
            "versions": d.get("versions", 0) or 0,
            "dl_delta": dl - pv,
            "proj_delta": (d.get("projects", 0) or 0) - pr,
            "net_new": (d.get("new", 0) or 0) - (d.get("removed", 0) or 0),
        })
    status_counts = days[-1].get("status", {}) if days else {}
    gainers = {}
    losers = {}
    velocity = {}
    today = days[-1]["date"] if days else None
    for win in (7, 30, 90):
        cutoff = (date.fromisoformat(today) - timedelta(days=win)).isoformat() if today else None
        base, since = state_at(cutoff) if cutoff else (None, None)
        if base is None:
            gainers[str(win)] = {"since": None, "rows": []}
            losers[str(win)] = {"since": None, "rows": []}
            velocity[str(win)] = {"since": None, "days": win, "rows": []}
            continue
        gains = []
        for i, v in cur.items():
            if i in base:
                g = v[0] - base[i][0]
                if g > 0:
                    old_dl = base[i][0]
                    pct = (g / old_dl * 100) if old_dl > 0 else float("inf")
                    gains.append((g, pct, i, v[0]))
        gains.sort(key=lambda x: -x[0])
        gainers[str(win)] = {
            "since": since,
            "rows": [
                {"id": i, "title": cur[i][3], "slug": cur[i][4],
                 "gained": g, "downloads": dl, "pct": round(pct, 2) if pct != float("inf") else None}
                for g, pct, i, dl in gains[:50]
            ],
        }
        losses = []
        for i, v in cur.items():
            if i in base:
                l = base[i][0] - v[0]
                if l > 0:
                    losses.append((l, i, v[0]))
        losses.sort(key=lambda x: -x[0])
        losers[str(win)] = {
            "since": since,
            "rows": [
                {"id": i, "title": cur[i][3], "slug": cur[i][4],
                 "lost": l, "downloads": dl}
                for l, i, dl in losses[:30]
            ],
        }
        velocity[str(win)] = {
            "since": since, "days": win,
            "rows": [
                {"id": i, "title": cur[i][3], "slug": cur[i][4],
                 "velocity": round(g / max(win, 1), 1), "gained": g, "downloads": dl}
                for g, pct, i, dl in gains[:50]
            ],
        }
    daily_diffs = []
    for s in series[1:]:
        daily_diffs.append({
            "date": s["date"], "dl_delta": s["dl_delta"],
            "new": s["new"], "removed": s["removed"],
            "changed": s["changed"], "net_new": s["net_new"],
        })
    days = index.get("days", [])
    prevmap = {d["date"]: (days[i - 1]["date"] if i > 0 else None)
               for i, d in enumerate(days)}
    delta_files = sorted((HIST / "deltas").glob("delta-*.json")) if (HIST / "deltas").exists() else []
    keep = {f.stem[len("delta-"):] for f in delta_files[-30:]}
    for f in delta_files[:-30]:
        d = f.stem[len("delta-"):]
        for stale in (PUB / ("diff-%s.json" % d), PUB / ("diff-%s.json.gz" % d)):
            if stale.exists():
                stale.unlink()
    for f in delta_files:
        d = f.stem[len("delta-"):]
        if d not in keep:
            continue
        part = load_json(f) or {}
        ids, ddl, df, dv = [], [], [], []
        for r in part.get("changed", []):
            if len(r) < 7:
                continue
            ids.append(r[0])
            ddl.append(r[-3] - r[1])
            df.append(r[-2] - r[2])
            dv.append(r[-1] - r[3])
        new_rows = [[r[0], r[1], r[2], r[3]] for r in part.get("new", [])]
        removed = part.get("removed", [])
        top_changed = []
        for k in [k for k in sorted(range(len(ids)), key=lambda k: -ddl[k]) if ddl[k] > 0][:50]:
            meta = cur.get(ids[k])
            top_changed.append({"id": ids[k], "title": meta[3] if meta else None,
                                "slug": meta[4] if meta else None, "gained": ddl[k]})
        new_rows.sort(key=lambda r: -r[1])
        top_new = []
        for r in new_rows[:25]:
            meta = cur.get(r[0])
            top_new.append({"id": r[0], "title": meta[3] if meta else None,
                            "slug": meta[4] if meta else None,
                            "downloads": r[1], "followers": r[2], "versions": r[3]})
        (PUB / ("diff-%s.json" % d)).unlink(missing_ok=True)
        write_gz(PUB / ("diff-%s.json.gz" % d),
                 {"v": 1, "date": d, "base": prevmap.get(d),
                  "counts": {"new": len(new_rows), "changed": len(ids),
                             "removed": len(removed)},
                  "ch": [ids, ddl, df, dv], "new": new_rows, "rm": removed,
                  "top_changed": top_changed, "top_new": top_new,
                  "totals": part.get("totals", {})})
    hours = read_hours()
    if hours:
        hseries = []
        for idx, h in enumerate(hours):
            prev = hours[idx - 1] if idx > 0 else None
            dl = h.get("downloads", 0) or 0
            pv = (prev.get("downloads", 0) or 0) if prev else dl
            pr = (prev.get("projects", 0) or 0) if prev else (h.get("projects", 0) or 0)
            hseries.append({
                "date": h.get("date"), "projects": h.get("projects", 0) or 0,
                "downloads": dl, "new": h.get("new", 0) or 0,
                "removed": h.get("removed", 0) or 0, "changed": h.get("changed", 0) or 0,
                "followers": h.get("followers", 0) or 0, "versions": h.get("versions", 0) or 0,
                "dl_delta": dl - pv, "proj_delta": (h.get("projects", 0) or 0) - pr,
                "net_new": (h.get("new", 0) or 0) - (h.get("removed", 0) or 0),
            })
        series = hseries
    keys = [re.sub(r"\.json(\.gz)?$", "", p.name[len("diff-"):])
            for p in PUB.glob("diff-*") if p.name.endswith((".json", ".json.gz"))]
    keys = sorted(set(keys), reverse=True)
    write_if_changed(
        PUB / "analytics.json",
        json.dumps({
            "updated": int(time.time()),
            "granularity": "hour" if hours else "day",
            "series": series,
            "daily_diffs": daily_diffs,
            "diffs": keys,
            "status_counts": status_counts,
            "gainers": gainers,
            "losers": losers,
            "velocity": velocity,
        }),
    )


def snapshot(c):
    HIST.mkdir(exist_ok=True)
    (HIST / "deltas").mkdir(exist_ok=True)
    PUB.mkdir(exist_ok=True)
    today = os.environ.get("MR_SNAP_DATE") or time.strftime("%Y-%m-%d", time.gmtime())
    idx_path = HIST / "index.json"
    index = load_json(idx_path) or {"days": []}
    cur = lean_map(c)
    try:
        log("mod series %s" % json.dumps(record_mod_series(c, today, cur)))
    except (sqlite3.Error, OSError, ValueError) as e:
        log("mod series skipped: %s" % e)
    totals = {
        "projects": len(cur),
        "downloads": sum(v[0] for v in cur.values()),
        "followers": sum(v[1] for v in cur.values()),
        "versions": sum(v[2] for v in cur.values()),
    }
    sc = {}
    for v in cur.values():
        k = v[6] or "unknown"
        sc[k] = sc.get(k, 0) + 1
    totals["status"] = sc
    prev = load_json(HIST / "latest.json")
    if prev is None:
        rows = lean_rows(cur)
        payload = json.dumps({"date": today, "rows": rows})
        write_if_changed(HIST / "latest.json", payload)
        write_if_changed(HIST / ("full-%s.json" % today), payload)
        index = {"days": [{"date": today, "new": 0, "changed": 0, "removed": 0,
                           "projects": totals["projects"], "downloads": totals["downloads"],
                           "followers": totals["followers"], "versions": totals["versions"],
                            "status": totals["status"]}],
                 "last_full": today, "census": today}
        write_census(c, int(time.time()))
        write_if_changed(idx_path, json.dumps(index, indent=1))
        emit_analytics(index, cur)
        log("snapshot baseline %s" % json.dumps(totals))
        return totals
    if any(d["date"] == today for d in index.get("days", [])):
        for d in index["days"]:
            if d["date"] == today:
                d["projects"] = totals["projects"]
                d["downloads"] = totals["downloads"]
                d["followers"] = totals["followers"]
                d["versions"] = totals["versions"]
                d["status"] = totals["status"]
        write_if_changed(idx_path, json.dumps(index, indent=1))
        emit_analytics(index, cur)
        log("snapshot already recorded for %s, analytics refreshed" % today)
        return totals
    prev_rows = {r[0]: (r[1], r[2], r[3]) for r in prev.get("rows", [])}
    new, changed, removed = [], [], []
    for i, v in cur.items():
        old = prev_rows.get(i)
        if old is None:
            new.append([i, v[0], v[1], v[2], v[3], v[4], v[5]])
        elif old != (v[0], v[1], v[2]):
            changed.append([i, old[0], old[1], old[2], v[0], v[1], v[2]])
    for i in prev_rows:
        if i not in cur:
            removed.append(i)
    new.sort(key=lambda r: -r[1])
    changed.sort(key=lambda r: -r[1])
    write_if_changed(
        HIST / "deltas" / ("delta-%s.json" % today),
        json.dumps({"date": today, "new": new, "changed": changed,
                    "removed": removed, "totals": totals}),
    )
    rows = lean_rows(cur)
    write_if_changed(HIST / "latest.json", json.dumps({"date": today, "rows": rows}))
    index.setdefault("days", []).append(
        {"date": today, "new": len(new), "changed": len(changed),
         "removed": len(removed), "projects": totals["projects"],
         "downloads": totals["downloads"], "followers": totals["followers"],
         "versions": totals["versions"], "status": totals["status"]}
    )
    last_full = index.get("last_full")
    if not last_full or (date.fromisoformat(today) - date.fromisoformat(last_full)).days >= 7:
        write_if_changed(HIST / ("full-%s.json" % today), json.dumps({"date": today, "rows": rows}))
        index["last_full"] = today
    index["census"] = today
    write_census(c, int(time.time()))
    write_if_changed(idx_path, json.dumps(index, indent=1))
    emit_analytics(index, cur)
    log("snapshot %s new=%d changed=%d removed=%d" % (today, len(new), len(changed), len(removed)))
    return totals


def emit_hourly(c):
    ts = int(time.time())
    hr = time.strftime("%Y-%m-%dT%H", time.gmtime(ts))
    ch_ids, ch_d, ch_f, ch_v, new_rows = [], [], [], [], []
    for i, a, b, cc, kind in c.execute("SELECT id, a, b, c, kind FROM changelog"):
        if kind == "new":
            new_rows.append([i, a or 0, b or 0, cc or 0])
        else:
            ch_ids.append(i)
            ch_d.append(a or 0)
            ch_f.append(b or 0)
            ch_v.append(cc or 0)
    cur = lean_map(c)
    totals = {
        "projects": len(cur),
        "downloads": sum(v[0] for v in cur.values()),
        "followers": sum(v[1] for v in cur.values()),
        "versions": sum(v[2] for v in cur.values()),
    }
    sc = {}
    for v in cur.values():
        k = v[6] or "unknown"
        sc[k] = sc.get(k, 0) + 1
    hours = read_hours()
    hours.append({"t": ts, "date": hr, "projects": totals["projects"],
                  "downloads": totals["downloads"], "followers": totals["followers"],
                  "versions": totals["versions"], "status": sc,
                  "new": len(new_rows), "changed": len(ch_ids), "removed": 0})
    write_if_changed(HIST / "hours.json", json.dumps(hours, indent=1))

    def brief(i):
        r = c.execute("SELECT title, slug FROM projects WHERE id=?", (i,)).fetchone()
        return (r[0], r[1]) if r else (None, None)

    top_changed = []
    for k in [k for k in sorted(range(len(ch_ids)), key=lambda k: -ch_d[k]) if ch_d[k] > 0][:50]:
        t, s = brief(ch_ids[k])
        top_changed.append({"id": ch_ids[k], "title": t, "slug": s, "gained": ch_d[k]})
    new_rows.sort(key=lambda r: -r[1])
    top_new = []
    for r in new_rows[:25]:
        t, s = brief(r[0])
        top_new.append({"id": r[0], "title": t, "slug": s,
                        "downloads": r[1], "followers": r[2], "versions": r[3]})
    index = load_json(HIST / "index.json") or {}
    diff = {"v": 1, "t": ts, "base": index.get("census"),
            "counts": {"new": len(new_rows), "changed": len(ch_ids), "removed": 0},
            "ch": [ch_ids, ch_d, ch_f, ch_v], "new": new_rows, "rm": [],
            "top_changed": top_changed, "top_new": top_new, "totals": totals}
    write_gz(PUB / ("diff-%s.json.gz" % hr), diff)
    keep = int(os.environ.get("MR_HDIFF_KEEP", "0") or 0)
    if keep > 0:
        for old in sorted(PUB.glob("diff-*.json.gz"), key=lambda p: p.name,
                          reverse=True)[keep:]:
            old.unlink()
    c.execute("DELETE FROM changelog")
    c.commit()
    emit_analytics(index, cur)
    try:
        emit_mod_trends(c)
    except (sqlite3.Error, OSError, ValueError) as e:
        log("mod trends skipped: %s" % e)
    log("hourly diff %s changed=%d new=%d" % (hr, len(ch_ids), len(new_rows)))
    return diff["counts"]


def facet_label(kind, val):
    if kind == "type":
        return val or "unknown"
    if kind == "client_side":
        return {"required": "client required", "optional": "client optional",
                "unsupported": "client unsupported", "unknown": "client unknown"}.get(val, val or "unknown")
    if kind == "server_side":
        return {"required": "server required", "optional": "server optional",
                "unsupported": "server unsupported", "unknown": "server unknown"}.get(val, val or "unknown")
    if kind == "status":
        return {"approved": "approved", "archived": "archived", "listed": "listed",
                "private": "private", "draft": "draft"}.get(val, val or "unknown")
    if kind == "license":
        return val or "unknown"
    return val or "unknown"


def org_labels(c):
    tally = {}
    for org, uname in c.execute(
        """SELECT p.org, pt.username FROM projects p
           JOIN team_projects tp ON tp.project_id = p.id
           JOIN project_team pt ON pt.project_id = tp.team_id
           WHERE p.org IS NOT NULL AND p.org <> '' AND pt.username IS NOT NULL"""
    ):
        tally.setdefault(org, {}).setdefault(uname, 0)
        tally[org][uname] += 1
    out = {}
    for org, people in tally.items():
        best = sorted(people.items(), key=lambda kv: (-kv[1], kv[0]))
        names = [n for n, _ in best[:2]]
        out[org] = " / ".join(names)
    for org, title in c.execute(
        """SELECT org, title FROM projects WHERE org IS NOT NULL AND org <> ''
           AND title IS NOT NULL AND title <> ''
           ORDER BY downloads DESC"""
    ):
        if org not in out:
            out[org] = title
    return out


def emit_facets(c, projects_by_dl):
    orgname = org_labels(c)
    facet_counts = {"category": {}, "type": {}, "client_side": {},
                    "server_side": {}, "status": {}, "license": {}, "org": {}}
    for r in c.execute(
        """SELECT categories, additional_categories, project_type, client_side,
             server_side, status, license, org FROM projects"""
    ):
        cats = jload(r[0], []) + jload(r[1], [])
        for k in set(cats):
            facet_counts["category"][k] = facet_counts["category"].get(k, 0) + 1
        ptype = r[2] or "unknown"
        facet_counts["type"][ptype] = facet_counts["type"].get(ptype, 0) + 1
        cs = jload(r[3], None)
        cs = (cs[0] if isinstance(cs, list) and cs else cs) or "unknown"
        facet_counts["client_side"][cs] = facet_counts["client_side"].get(cs, 0) + 1
        ss = jload(r[4], None)
        ss = (ss[0] if isinstance(ss, list) and ss else ss) or "unknown"
        facet_counts["server_side"][ss] = facet_counts["server_side"].get(ss, 0) + 1
        status = r[5] or "unknown"
        facet_counts["status"][status] = facet_counts["status"].get(status, 0) + 1
        if (r[6] or "").strip():
            lic = r[6].strip()
            facet_counts["license"][lic] = facet_counts["license"].get(lic, 0) + 1
        if (r[7] or "").strip():
            og = r[7].strip()
            facet_counts["org"][og] = facet_counts["org"].get(og, 0) + 1

    org_dl = {}
    for org, dl in c.execute(
        """SELECT org, SUM(downloads) FROM projects
           WHERE org IS NOT NULL AND org <> '' GROUP BY org"""
    ):
        org_dl[org] = dl or 0

    facets = []
    for kind, counts in facet_counts.items():
        for val, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            label = facet_label(kind, val)
            entry = {"kind": kind, "value": val, "label": label, "count": n}
            if kind == "org":
                label = orgname.get(val, val)
                entry["label"] = label
                entry["downloads"] = org_dl.get(val, 0)
            facets.append(entry)
    facets.sort(key=lambda f: (f["kind"], -f["count"]))
    write_if_changed(PUB / "facets.json", json.dumps({"facets": facets}))
    return facets


def emit_facet_boards(c, projects_by_id, facets, per=100, full_upto=1000, cap=400):
    groups = {}
    for r in c.execute(
        """SELECT id, categories, additional_categories, project_type, client_side,
             server_side, status, license, org FROM projects"""
    ):
        pid = r[0]
        cats = set(jload(r[1], []) + jload(r[2], []))
        ptype = r[3] or "unknown"
        cs = jload(r[4], None)
        cs = (cs[0] if isinstance(cs, list) and cs else cs) or "unknown"
        ss = jload(r[5], None)
        ss = (ss[0] if isinstance(ss, list) and ss else ss) or "unknown"
        status = r[6] or "unknown"
        license = (r[7] or "").strip() or None
        org = (r[8] or "").strip() or None
        keys = [("category", k) for k in cats]
        keys.append(("type", ptype))
        keys.append(("client_side", cs))
        keys.append(("server_side", ss))
        keys.append(("status", status))
        if license:
            keys.append(("license", license))
        if org:
            keys.append(("org", org))
        for k in keys:
            groups.setdefault(k, []).append(pid)

    for old in PUB.glob("facets-*.json"):
        old.unlink()
    boards = []
    for (kind, val), pids in groups.items():
        pids.sort(key=lambda p: -(projects_by_id.get(p, {}).get("downloads") or 0))
        n = len(pids) if len(pids) <= full_upto else min(cap, len(pids))
        top = [projects_by_id[p] for p in pids[:n] if p in projects_by_id]
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(val))[:50]
        sig = hashlib.sha1(str(val).encode("utf-8")).hexdigest()[:8]
        name = "facets-%s-%s-%s.json" % (kind, safe, sig)
        write_if_changed(PUB / name, json.dumps(top))
        boards.append({"kind": kind, "value": val, "file": name,
                       "members": len(pids), "shown": len(top)})
    write_if_changed(PUB / "facets-index.json", json.dumps({"boards": boards}))
    return boards


def full(client, c):
    log("census start")
    stats = client.get("/statistics") or {}
    log("modrinth totals %s" % stats)
    seen_all = 0
    for index in ("downloads", "newest", "updated"):
        n, total = walk(client, c, index)
        seen_all += n
        log("rail %s seen %d total_hits %d" % (index, n, total))
    rows = c.execute("SELECT team FROM projects").fetchall()
    members = collect_teams(client, c, [t[0] for t in rows])
    link_teams(c)
    log("team members %d" % members)
    uids = [r[0] for r in c.execute("SELECT DISTINCT user_id FROM project_team WHERE user_id IS NOT NULL")]
    uids += [r[0] for r in c.execute("SELECT author_id FROM projects WHERE author_id IS NOT NULL")]
    users = collect_users(client, c, uids)
    log("users %d" % users)
    edges = crawl_deps(client, c, TOP_N)
    log("dep edges %d" % edges)
    log("cache hits %d" % client.saved)


def live(client, c):
    hot = client.get("/search?index=downloads&limit=100&offset=0") or {}
    ingest_ids(client, c, [h.get("project_id") for h in hot.get("hits", [])], int(time.time()))
    rows = c.execute("SELECT team FROM projects WHERE team IS NOT NULL").fetchall()
    collect_teams(client, c, [t[0] for t in rows])
    edges = crawl_deps(client, c, min(TOP_N, 50))
    log("live edges %d" % edges)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--web", action="store_true")
    ap.add_argument("--emit", action="store_true")
    ap.add_argument("--users", action="store_true")
    ap.add_argument("--deps", action="store_true")
    ap.add_argument("--hourly", action="store_true")
    ap.add_argument("--snapshot", action="store_true")
    a = ap.parse_args()
    client = PacedClient()
    c = sqlite3.connect(DB)
    c.execute("PRAGMA journal_mode=WAL")
    init_db(c)
    if a.full:
        full(client, c)
    if a.live:
        live(client, c)
    if a.hourly:
        stats = hourly(client, c)
        log("hourly %s" % json.dumps(stats))
    if a.emit:
        link_teams(c)
    if a.users:
        link_teams(c)
        uids = [r[0] for r in c.execute("SELECT DISTINCT user_id FROM project_team")]
        got = collect_users(client, c, uids)
        c.commit()
        log("users %d" % got)
    if a.deps:
        edges = crawl_deps(client, c, TOP_N)
        c.commit()
        log("deps edges %d" % edges)
    if a.snapshot:
        held = c.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        if held < MIN_PUBLISH:
            log("refusing to snapshot: only %d projects in database" % held)
        else:
            snap = snapshot(c)
            c.commit()
            log("snapshot %s" % json.dumps(snap))
    if a.web or a.full or a.live or a.emit or a.users or a.deps or a.hourly:
        c.commit()
        held = c.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        if held < MIN_PUBLISH:
            log("refusing to publish: only %d projects in database" % held)
            c.close()
            raise SystemExit(1)
        light = a.live and not (a.full or a.emit or a.users or a.deps or a.web)
        meta = emit(client, c, light=light)
        c.commit()
        log("emitted %s" % json.dumps(meta))
    c.close()


if __name__ == "__main__":
    main()
