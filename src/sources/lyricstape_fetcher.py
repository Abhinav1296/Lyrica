import re
from urllib.parse import quote_plus, urljoin
from bs4 import BeautifulSoup

from .base_fetcher import BaseFetcher, build_result, get_http_client

TELUGU_RE = re.compile(r"[\u0C00-\u0C7F]")

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _has_telugu(s: str) -> bool:
    return bool(TELUGU_RE.search(s or ""))

class LyricsTapeFetcher(BaseFetcher):
    source_name = "lyricstape"

    BASE = "https://www.lyricstape.com"

    async def _search_page(self, artist: str, song: str) -> str | None:
        """
        LyricsTape search bar UI may be broken, but WordPress sites often support:
        https://www.lyricstape.com/?s=<query>
        We try that. If it returns results, we pick the best song page link.
        """
        client = get_http_client()
        q = quote_plus(f"{song} {artist}".strip())
        url = f"{self.BASE}/?s={q}"
        resp = await client.get(url)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        links = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "lyricstape.com" not in href:
                continue
            # heuristic: song lyric pages usually contain "song-lyrics"
            if "song-lyrics" in href:
                links.append(href)

        if not links:
            return None

        # pick the best match by simple contains scoring
        song_n = _norm(song)
        best = None
        best_score = -1
        for href in links:
            h = _norm(href)
            score = 0
            if song_n and song_n.replace(" ", "-") in h:
                score += 3
            if song_n and song_n in h:
                score += 2
            if "song-lyrics" in h:
                score += 1
            if score > best_score:
                best_score = score
                best = href

        return best

    async def _extract_english_lyrics(self, html: str) -> str | None:
        soup = BeautifulSoup(html, "lxml")

        # Most WP themes keep content in .entry-content
        node = soup.select_one(".entry-content") or soup.select_one("article") or soup.select_one("main") or soup.body
        if not node:
            return None

        # remove junk
        for tag in node.select("script, style, nav, footer, header, form, button"):
            tag.decompose()

        text = node.get_text("\n")
        lines = [ln.strip() for ln in text.splitlines()]
        lines = [ln for ln in lines if ln]

        # filter: keep only lines that are NOT Telugu script
        eng = []
        for ln in lines:
            # drop noisy UI lines
            low = ln.lower()
            if any(k in low for k in ["lyricstape", "album", "movie", "share", "copyright", "privacy", "cookies"]):
                continue
            if _has_telugu(ln):
                continue
            # keep roman-ish lines
            if len(ln) > 1:
                eng.append(ln)

        # Heuristic: lyrics usually have multiple lines
        if len(eng) < 4:
            return None

        # de-duplicate consecutive duplicates
        out = []
        for ln in eng:
            if not out or out[-1] != ln:
                out.append(ln)

        return "\n".join(out).strip()

    async def fetch(self, artist: str, song: str, timestamps: bool = False):
        # LyricsTape is plain-only
        client = get_http_client()

        page = await self._search_page(artist, song)
        if not page:
            return None

        resp = await client.get(page)
        if resp.status_code != 200:
            return None

        lyrics = await self._extract_english_lyrics(resp.text)
        if not lyrics:
            return None

        return build_result(
            source="lyricstape",
            artist=artist,
            title=song,
            lyrics=lyrics,
            timed_lyrics=None,
            has_timestamps=False,
            url=page,
        )
