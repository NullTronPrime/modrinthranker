import argparse
import json
import os
import random
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CACHE = DATA / "cache"
PUB = Path(os.environ.get("MR_PUB", str(ROOT / "docs")))
for _d in (DATA, CACHE, PUB):
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
            p.get("downloads", 0) or 0, p.get("followers", 0) or 0,
            len(p.get("versions") or []) or p.get("versions", 0) or 0,
            p.get("status"), p.get("published"), p.get("updated"),
            p.get("team"), p.get("organization"), p.get("author"), p.get("author_id"),
            p.get("icon_url"), p.get("color"), 1 if p.get("featured") else 0,
            p.get("thread_id"), license_id, p.get("issues_url"), p.get("source_url"),
            p.get("wiki_url"), p.get("discord_url"), len(p.get("gallery") or []),
            checked,
        ),
    )


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


def crawl_deps(client, c, top_k):
    rows = c.execute("SELECT id FROM projects ORDER BY downloads DESC LIMIT ?", (top_k,)).fetchall()
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
                    target = dep.get("project_id") or dep.get("version_id")
                    if target:
                        c.execute("INSERT OR IGNORE INTO deps VALUES(?,?,?)",
                                  (pid, target, dep.get("dependency_type")))
                        edges += 1
        if n % 50 == 0:
            c.commit()
            log("  deps %d/%d edges %d" % (n, len(rows), edges))
    return edges


def emit(client, c, light=False):
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
            "status": r[11], "published": r[12], "updated": r[13],
            "org": r[14], "author": r[15], "author_id": r[16],
            "members": members, "contributors": max(len(members) - 1, 0),
            "icon_url": r[17], "color": r[18], "featured": r[19],
            "license": r[20], "issues_url": r[21], "source_url": r[22],
            "wiki_url": r[23], "discord_url": r[24], "gallery": r[25],
        })

    dl_rows = c.execute(
        """SELECT pt.user_id, SUM(p.downloads), COUNT(DISTINCT p.id),
             MAX(u.username), MAX(u.name), MAX(u.followers), MAX(u.joined)
          FROM team_projects tp
          JOIN project_team pt ON pt.project_id = tp.team_id
          JOIN projects p ON p.id = tp.project_id
          LEFT JOIN users u ON u.id = pt.user_id
          GROUP BY pt.user_id"""
    ).fetchall()

    authors = []
    for uid, total_dl, np_, uname, nm, followers, joined in dl_rows:
        if not np_:
            continue
        authors.append({
            "id": uid, "username": uname, "name": nm,
            "downloads": total_dl or 0, "projects": np_,
            "downloads_per_project": round((total_dl or 0) / np_, 1),
            "followers": followers or 0, "joined": joined,
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
        (PUB / "top.json").write_text(json.dumps(projects[:250]), "utf-8")
        (PUB / "meta.json").write_text(json.dumps(meta, indent=1), "utf-8")
        log("emitted light %s" % json.dumps(meta))
        return meta
    for stale in PUB.glob("projects-*.json"):
        stale.unlink()
    shards = []
    shard_size = SHARD_SIZE
    for start in range(0, len(projects), shard_size):
        chunk = projects[start : start + shard_size]
        name = "projects-%04d.json" % (start // shard_size)
        (PUB / name).write_text(json.dumps(chunk), "utf-8")
        shards.append({"file": name, "count": len(chunk), "offset": start})
    (PUB / "projects-index.json").write_text(
        json.dumps({"count": len(projects), "shard_size": shard_size, "shards": shards}),
        "utf-8",
    )
    (PUB / "top.json").write_text(json.dumps(projects[:250]), "utf-8")
    meta["project_shards"] = len(shards)
    (PUB / "authors.json").write_text(json.dumps(authors), "utf-8")
    (PUB / "teams.json").write_text(json.dumps(teams), "utf-8")
    (PUB / "deps.json").write_text(json.dumps(deps), "utf-8")
    (PUB / "categories.json").write_text(
        json.dumps({"categories": cat_counts, "types": type_counts}), "utf-8"
    )
    (PUB / "meta.json").write_text(json.dumps(meta, indent=1), "utf-8")
    return meta


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
    a = ap.parse_args()
    client = PacedClient()
    c = sqlite3.connect(DB)
    c.execute("PRAGMA journal_mode=WAL")
    init_db(c)
    if a.full:
        full(client, c)
    if a.live:
        live(client, c)
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
    if a.web or a.full or a.live or a.emit or a.users or a.deps:
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
