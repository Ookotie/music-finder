# Music Finder — Niche Artist Discovery Pipeline

Automated weekly pipeline that discovers emerging/underground artists matching Oni's musical taste, builds a Spotify playlist, and sends a Telegram notification. Integrated into **oni-hub** as a monitor module — not a standalone service.

---

## Integration Model

Music Finder follows the same pattern as the literature monitor. Production code lives inside oni-hub:

| Component | Location in oni-hub | Pattern reference |
|-----------|-------------------|-------------------|
| Discovery engine, Spotify client, scanner | `src/monitors/music/` | `src/monitors/literature/` |
| Bot commands (`/music`, `/genres`, etc.) | `src/bot/handlers.py` | Existing `cmd_scan` handler |
| Recommendation history (optional) | `src/notion/music.py` | `src/notion/tasks.py` |
| Scheduler registration | `src/main.py` | `start_literature_monitor(scheduler)` |
| Config & env vars | `src/config.py` | Existing `LITERATURE_SCAN_*` vars |
| Error tracking | All modules | `track_error(e, context="music.discovery")` |

The `music-finder/` directory holds this CLAUDE.md and standalone dev/test scripts. All deployable code goes into oni-hub.

### Module Structure (inside oni-hub)

```
oni-hub/src/monitors/music/
├── __init__.py
├── config.py           # Tunable defaults, genre weights, tier boundaries
├── scheduler.py        # start_music_monitor(scheduler) — CronTrigger Friday 6 PM
├── scanner.py          # Orchestrator: taste → discover → score → playlist → notify
├── taste_profiler.py   # Pull and weight user's listening data
├── discovery.py        # Multi-source candidate gathering
├── scorer.py           # Composite scoring algorithm
├── playlist_builder.py # Spotify playlist creation/archival
├── spotify_client.py   # Spotipy wrapper with auto-refresh
└── lastfm_client.py    # Last.fm API wrapper
```

### Bot Commands to Register

Add to `build_application()` in `src/bot/handlers.py`:

| Command | Purpose |
|---------|---------|
| `/music` | Trigger on-demand scan or show latest playlist |
| `/genres` | Display current genre weight map |
| `/boost <genre>` | Increase a genre's weight in scoring |
| `/suppress <genre>` | Decrease a genre's weight |
| `/threshold <number>` | Change monthly listener ceiling (default 100K) |
| `/history` | Show last 4 weeks of playlists |

Inline keyboard feedback on recommendations: thumbs up (boost genres), thumbs down (penalize), star (add to seeds). Callback pattern: `mf:` prefix.

---

## Pipeline Stages

```
1. Taste Profiling    → Pull top artists/tracks/follows from Spotify, build weighted genre map
2. Discovery          → Crawl related artists 2-3 degrees out (Spotify + Last.fm), genre-seeded recs
3. Scoring            → Genre match + popularity tier + source diversity + momentum → composite score
4. Playlist Building  → Top tracks from top 20-30 artists → new Spotify playlist, archive old
5. Notification       → Telegram message with link, stats, top 5 highlights, trend notes
```

Each stage is isolated in try-except with `track_error()`. Partial source failure (e.g., Last.fm down) does not block the pipeline — continue with remaining sources.

---

## Required Environment Variables

```bash
# Spotify OAuth2 (Authorization Code flow)
SPOTIFY_CLIENT_ID=           # required
SPOTIFY_CLIENT_SECRET=       # required
SPOTIFY_REFRESH_TOKEN=       # required — enables unattended runs

# Last.fm (API key only, no OAuth)
LASTFM_API_KEY=              # required

# Music Finder scheduling (added to oni-hub config.py)
MUSIC_SCAN_DAY=fri           # default: fri
MUSIC_SCAN_HOUR=18           # default: 18 (6 PM ET)

# Tunable defaults (can also be changed via bot commands)
MAX_MONTHLY_LISTENERS=100000
PLAYLIST_SIZE=25
ARTIST_COOLDOWN_WEEKS=4
```

Spotify scopes needed: `user-top-read`, `user-library-read`, `user-follow-read`, `playlist-modify-public`, `playlist-modify-private`.

Telegram vars (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`) are shared with oni-hub — no duplication.

---

## SQLite Schema (7 Tables)

Stored in `data/music_finder.db` (inside oni-hub's data directory).

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `taste_profile` | Weighted genre preferences | genre, weight |
| `seed_artists` | Known artists from user's library | spotify_id, name, genres (JSON), monthly_listeners |
| `candidates` | Discovered artists with scores | spotify_id, discovery_source, genre_match_score, momentum_score, composite_score |
| `playlist_history` | Weekly playlist records | playlist_id, playlist_name, created_at |
| `recommendations` | Tracks placed in playlists | artist_spotify_id, track_spotify_id, playlist_history_id |
| `feedback` | User thumbs up/down/love | artist_spotify_id, feedback_type |
| `listener_snapshots` | Monthly listener tracking over time | artist_spotify_id, monthly_listeners, snapshot_date |

Full DDL is in the project spec (plan transcript). Tables are created on first run via `CREATE TABLE IF NOT EXISTS`.

---

## Scoring Algorithm

```
composite_score = (
    genre_match_score * 0.4      # 0-1, overlap with user's weighted genre map
  + popularity_score * 0.3       # favors "breakout zone" (50K-250K listeners)
  + source_diversity_bonus * 0.2 # found via multiple sources = higher confidence
  + momentum_score * 0.1         # Phase 5: >20% week-over-week listener growth
)
```

- Minimum genre match threshold: 0.3 (skip candidates below this)
- Popularity tiers: Underground (<10K), Growing indie (10K-50K), **Breakout zone (50K-250K)**, Mid-tier (250K-1M), Mainstream (1M+)
- Cooldown: skip artists recommended within the last `ARTIST_COOLDOWN_WEEKS` weeks
- Deduplication: merge candidates found via multiple sources, count sources for diversity bonus

---

## Design Constraints

- **Cooldown period**: Default 4 weeks before re-recommending an artist (configurable)
- **Listener threshold**: Default <100K monthly listeners; adjustable via `/threshold` or env var
- **Partial failure tolerance**: Any single source can fail without killing the pipeline
- **Low candidate fallback**: If <20 candidates pass filters, temporarily raise the listener threshold
- **Token auto-refresh**: Spotify refresh token handles expiry automatically; retry once on 401
- **Quiet execution**: Runs on schedule, only notifies on success or complete failure

---

## Phased Build Plan

### Phase 1: Spotify Auth & Taste Profiling
- Spotify OAuth2 setup (Authorization Code flow with refresh token)
- Taste profiler: top artists (3 time ranges), top tracks, followed artists
- Genre extraction and weighted map
- SQLite setup (`taste_profile`, `seed_artists`)
- **Deliverable**: Run script, see ranked genres and seed artists

### Phase 2: Discovery Engine
- Spotify related artists (2-3 degrees), recommendation endpoint with genre seeds
- Last.fm `artist.getSimilar`, `tag.getTopArtists`
- Candidate scoring (genre match, popularity, source diversity)
- SQLite storage (`candidates`)
- **Deliverable**: Ranked list of ~50 candidate artists with scores

### Phase 3: Playlist Builder
- Top tracks selection (1 per artist, top 20-30 artists)
- Spotify playlist creation with naming format `Niche Finds -- Week of {date}`
- Archive previous week's playlist
- History tracking with cooldown enforcement (`playlist_history`, `recommendations`)
- **Deliverable**: Fresh playlist appears in Spotify library

### Phase 4: Scheduling & Notifications
- Telegram notification (playlist link, stats, top 5 highlights, trends)
- Oni-hub integration: `start_music_monitor(scheduler)`, bot commands
- Error handling: partial source failure, token refresh, low-candidate fallback
- **Deliverable**: Fully hands-off — weekly playlist + Telegram notification

### Phase 5: Tuning & Extra Sources
- Inline keyboard feedback (thumbs up/down/love) stored in `feedback` table
- Bandcamp scraping by tag
- Music blog RSS feeds (Pitchfork, Stereogum, CVLT Nation, The Quietus, BrooklynVegan)
- Momentum detection via `listener_snapshots` (>20% WoW growth bonus)
- `/genres`, `/boost`, `/suppress`, `/threshold`, `/history` bot commands
- **Deliverable**: Recommendations improve weekly based on feedback and richer data
