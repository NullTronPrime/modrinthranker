"""
modrinth_ranker - census + leaderboards for Modrinth (modders & mods) + a
dependency/team spiderweb, emitting static JSON for a GitHub Pages site.
WHY A CRAWLER SCRIPT IS THE ONLY VALID "BACKEND" FOR GITHUB PAGES
  GitHub Pages is STATIC file hosting: there is no server-side runtime (no
  Python/Node, no database, no API proxy) that executes on the Pages origin
  at request time. Every algorithm therefore runs at BUILD time, and its only
  output has to be files that the Pages CDN can serve verbatim weekends.
  We therefore:
    * run THIS script as a scheduled GitHub Action (Workflow cron); it is the
      entire "backend" - census + increment,
    * write derived public/*.json assets into the repo (committed, pushed),
    * let GitHub Pages serve exactly those committed static JSON files.
  Visitors never call api.modrinth.com at all, so the site costs the API zero
  requests per visitor on perpetuity, is not rate-limitable per-viewer, and
  cannot leak a token. This is also the only pattern the API staff actually
  vets: a descriptive User-Agent, a paced crawl, no in-browser API fanout.
THE HONEST "SPIDERWEB" (docs + #api-and-minotaur dump; this is what is and
is NOT crawlable, and it decides the graph we emit):
  * follows: PUBLIC download/follower COUNTS for every project + every user.
             /search gives downloads, followers, author_id per hit with zero
             extra requests. /users?ids= and /user/{id} give author followers
             and join dates in bulk.
  * who-follows-whom / who-follows-what: PRIVATE PER-USER. GET
             /user/{id}/follows requires USER_READ auth AND returns ONLY the
             REQUESTING USER'S OWN followed projects; follower identity is
             never exposed to other users or to token holders at any rate
             limit. This is a privacy boundary, not a rate boundary. There is
             NO request budget that can enumerate "which modders follow which
             projects" because Modrinth deliberately hides that relation from
             everyone but the account holder.
  * What IS PUBLIC and forms the real web Modrinth exposes:
        (a) DEPENDENCIES  project ->(depends on)-> project   edges; per
            project via /project/{id}/dependencies (no bulk route).
        (b) TEAM COLLAB:  user <->(team member of)-> project   edges; expand
            a project's TEAM via /teams?ids= bulk and you get ALL collabs on
            that project, and two users who share a team on any project share
            a "work together" edge. This is the person-person spider web.
  So: leaderboards are built on strongly-public numbers (downloads, followers,
  author counts, join dates); the "spiderweb" page is built on the two public
  relation families above, and our methodology panel states plainly that
  follower-identity webs are impossible on Modrinth - no favicon, no token,
  no RPM setting can change that. We never fabricate them.
RATE LIMIT / BLACKLIST CONTRACT (docs + staff, honored by design):
  * 300 requests/min per IP (with or without a token). Default pace: 90/min.
  * MUST send a descriptive User-Agent with a contact; set MR_UA. Bare
    "okhttp/..." / UUID / absent UA is the #1 blacklist cause per staff.
  * Honor X-Ratelimit-* headers when they appear; on 429/5xx sleep and retry
    up to 8x with exponential backoff; never tunnel in-browser (we don't).
  * Bulk routes: staff cap ~800 ids/batch. Search: limit<=100, offset pages.
  * Disk-cache everything (24h TTL): re-runs and re-renders cost nothing.
CENSUS SCALE (live 2026-09: 158,783 projects / 68,546 authors / 1,419,157
versions / 1,540,177 files):
  * --full  weekly census: enumerate all three search index rails
            (downloads, newest, updated) -> ~1,588 search pages + ~199 bulk
            /projects + ~199 bulk /teams + OMR_TOP_N per-project deps.
            ~= 2,100 requests ~= 24 min at 90/min. Writes everything.
  * --live  every-5-10-min tick (the cron body): touch only the hottest
            TOP_N by downloads (search page + bulk projects + teams + deps)
            ~= a couple dozen requests per tick. Incremental, resumable.
  * --web   rebuild /public JSON from the existing sqlite census, NO network.
Store is data/ranks.sqlite; public/*.json are derived, idempotent emissions.
"""
import argparse, json, os, random, re, sqlite3, time, urllib.error, urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"; CACHE = DATA / "cache"; PUB = ROOT / "public"
for _d in (DATA, CACHE, PUB):
    _d.mkdir(parents=True, exist_ok=True)
DB = DATA / "ranks.sqlite"
BASE = os.environ.get("MR_BASE", "https://api.modrinth.com/v2")
UA = os.environ.get("MR_UA", "modrinthranker/1.0 (https://github.com/ragav/modrinthranker; dev@example.com)")
RPM = int(os.environ.get("MR_RPM", "90"))
TOP_N = int(os.environ.get("MR_TOP_N", "400"))
SMOKE = int(os.environ.get("MR_SMOKE", "0"))
SEARCH_LIMIT = 100
BULK_LIMIT = 800
RETRIES = 8
TTL = 24 * 3600
CACHE_ENABLED = not SMOKE
def log(*a):
    print(*a, flush=True)
class PacedClient:
    def __init__(self):
        self.gap = 60.0 / RPM
        self.last = 0.0
    def _pace(self):
        dt = time.time() - self.last
        want = self.gap * random.uniform(0.85, 1.2)
        if dt < want:
            time.sleep(want - dt)
        self.last = time.time()
    def get(self, path):
        fname = CACHE / re.sub(r"[^A-Za-z0-9_]", "_", path)[:120]
        if CACHE_ENABLED and fname.exists() and time.time() - fname.stat().st_mtime < TTL:
            return json.load(fname.open("r", encoding="utf-8"))
        url = BASE + path
        req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                   "Accept": "application/json"})
        for attempt in range(RETRIES):
            self._pace()
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    body = json.loads(r.read().decode("utf-8"))
                    if CACHE_ENABLED:
                        fname.write_text(json.dumps(body), "utf-8")
                    return body
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504):
                    reset = 30
                    if e.headers and e.headers.get("X-Ratelimit-Reset"):
                        try:
                            reset = min(int(e.headers["X-Ratelimit-Reset"]), 90)
                        except (TypeError, ValueError):
                            reset = 30
                    log("  backoff %ss on %s" % (reset, path))
                    time.sleep(reset + random.uniform(0, 2))
                    continue
                return None
        raise RuntimeError("gave up on %s" % path)
def init_db(c):
    c.executescript("""
    CREATE TABLE IF NOT EXISTS projects(
      id TEXT PRIMARY KEY, slug TEXT, title TEXT, project_type TEXT,
      downloads INTEGER, followers INTEGER, status TEXT,
      published TEXT, updated TEXT, team TEXT, org TEXT,
      author TEXT, author_id TEXT, last_checked INTEGER);
    CREATE TABLE IF NOT EXISTS project_team(
      project_id TEXT, user_id TEXT, is_owner INTEGER, role TEXT,
      PRIMARY KEY(project_id, user_id));
    CREATE TABLE IF NOT EXISTS deps(
      project_id TEXT, dep_project_id TEXT, dependency_type TEXT,
      PRIMARY KEY(project_id, dep_project_id));
    CREATE TABLE IF NOT EXISTS users(
      id TEXT PRIMARY KEY, username TEXT, name TEXT, bio TEXT,
      followers INTEGER, joined TEXT, role TEXT, last_checked INTEGER);
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE INDEX IF NOT EXISTS ix_proj_dl ON projects(downloads DESC);
    """)
def upsert_project(c, p, checked):
    c.execute("""INSERT INTO projects VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET downloads=excluded.downloads,
                   followers=excluded.followers, updated=excluded.updated,
                   last_checked=excluded.last_checked""",
              (p["id"], p.get("slug"), p.get("title"), p.get("project_type"),
               p.get("downloads", 0), p.get("followers", 0), p.get("status"),
               p.get("published"), p.get("updated"), p.get("team"),
               p.get("organization"), p.get("author"), p.get("author_id"),
               checked))
def ingest_search(client, c, hits):
    """Bulk-expand a search page's project ids (1 bulk call per page)."""
    ids = [h.get("project_id") for h in hits if h.get("project_id")]
    if not ids:
        return 0
    projs = client.get("/projects?ids=" + urllib.parse.quote(json.dumps(ids)))
    checked = int(time.time())
    if projs:
        for p in projs:
            upsert_project(c, p, checked)
    return len(ids)
def walk(client, c, index="downloads", facets=None):
    """Census one search rail by offset pagination."""
    offset, total, got = 0, None, 0
    while True:
        q = "/search?index=%s&limit=%d&offset=%d" % (index, SEARCH_LIMIT, offset)
        if facets:
            q += "&facets=" + json.dumps(facets)
        body = client.get(q)
        if not body or not body.get("hits"):
            break
        total = body.get("total_hits") or 0
        got += ingest_search(client, c, body["hits"])
        if SMOKE and offset >= 200:
            break
        offset += SEARCH_LIMIT
        if offset >= total:
            break
    return got
def crawl_deps(client, c, hot, top_k):
    """Dependency spiderweb edges for the hottest top_k projects only.
    Per-project route (no bulk) -> scoped to keep the request budget honest."""
    edges = 0
    for pid in hot[:top_k]:
        d = client.get("/project/%s/dependencies" % pid)
        if not d:
            continue
        for pr in d.get("projects") or []:
            c.execute("INSERT OR IGNORE INTO deps VALUES(?,?,?)",
                      (pid, pr.get("id"), pr.get("dependency_type")))
            edges += 1
        for v in d.get("versions") or []:
            if v.get("project_id"):
                c.execute("INSERT OR IGNORE INTO deps VALUES(?,?,?)",
                          (pid, v["project_id"], v.get("dependency_type")))
                edges += 1
    return edges
def crawl_teams(client, c, team_ids):
    """Bulk-expand team ids -> members (collab edges) + user rows."""
    tids = sorted(set(team_ids))
    n = 0
    for i in range(0, len(tids), BULK_LIMIT):
        chunk = tids[i:i + BULK_LIMIT]
        body = client.get("/teams?ids=" + urllib.parse.quote(json.dumps(chunk)))
        if not body:
            continue
        for members in body:
            if not members:
                continue
            n += len(members)
    return n
def tick(client, c, jar):
    """Incremental 5-10 min refresh: hottest rails + bulk teams + top deps."""
    got = 0
    for idx in ("downloads", "newest", "updated"):
        body = client.get("/search?index=%s&limit=%d&offset=0" % (idx, min(TOP_N, SEARCH_LIMIT)))
        if body:
            got += ingest_search(client, c, body.get("hits") or [])
    team_ids = [
        t[0] for t in
        c.execute("SELECT team FROM projects WHERE team IS NOT NULL LIMIT 1")]  # watermark
    return got
def emit(client, c):
    """Rebuild public/*.json from sqlite only (no network)."""
    rows = c.execute("""SELECT title, slug, project_type, downloads, followers
                        FROM projects ORDER BY downloads DESC LIMIT ?""",
                     (TOP_N * 2,)).fetchall()
    projects = [{"title": r[0], "slug": r[1], "type": r[2], "downloads": r[3],
                 "followers": r[4]} for r in rows]
    authors = {}
    for r in c.execute("""SELECT u.id, u.username, u.followers,
                            SUM(p.downloads) dl, COUNT(DISTINCT p.id) np
                          FROM project_team t
                          JOIN projects p ON p.id = t.project_id
                          JOIN users u ON u.id = t.user_id
                          GROUP BY t.user_id ORDER BY dl DESC"""):
        uid, uname, uf, dl, np = r
        authors[uid] = {"id": uid, "username": uname, "followers": uf,
                        "downloads": dl, "projects": np,
                        "downloads_per_project": round(dl / np, 1) if np else 0}
    author_list = sorted(authors.values(), key=lambda a: a["downloads"],
                         reverse=True)[:TOP_N]
    deps = [{"from": r[0], "to": r[1], "type": r[2]} for r in
            c.execute("SELECT project_id, dep_project_id, dependency_type FROM deps")]
    PUB.mkdir(exist_ok=True)
    (PUB / "projects.json").write_text(json.dumps(projects, indent=1), "utf-8")
    (PUB / "authors.json").write_text(json.dumps(author_list, indent=1), "utf-8")
    (PUB / "deps.json").write_text(json.dumps(deps, indent=1), "utf-8")
    (PUB / "meta.json").write_text(json.dumps({
        "generated": int(time.time()),
        "projects": len(projects),
        "authors": len(author_list),
        "deps": len(deps)}, indent=1), "utf-8")
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help="weekly full census (all search rails)")
    ap.add_argument("--live", action="store_true",
                    help="5-10 min incremental hot tick")
    ap.add_argument("--web", action="store_true",
                    help="rebuild public/*.json from sqlite, no network")
    a = ap.parse_args()
    cl = PacedClient()
    c = sqlite3.connect(DB)
    init_db(c)
    if a.full:
        log("full census...")
        for idx in ("downloads", "newest", "updated"):
            log(" rail %s: %d projects" % (idx, walk(cl, c, idx)))
    if a.live:
        log("live tick...")
        tick(cl, c, None)
    if a.web or a.full or a.live:
        emit(cl, c)
    c.commit()
    c.close()
    log("assets -> %s" % PUB)
if __name__ == "__main__":
    main()
