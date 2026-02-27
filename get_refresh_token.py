"""
Spotify OAuth2 Authorization Code Flow — One-time setup script.

Gets a refresh token for unattended API access.

Usage:
    1. Create a .env file with SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET
    2. Run: python get_refresh_token.py
    3. Browser opens → log in → authorize
    4. Script prints your refresh token — save it to .env as SPOTIFY_REFRESH_TOKEN
"""

import base64
import http.server
import json
import os
import secrets
import threading
import urllib.parse
import webbrowser

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env loading is optional if vars are set manually

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = "http://127.0.0.1:8888/callback"

SCOPES = " ".join([
    "user-top-read",
    "user-library-read",
    "user-follow-read",
    "playlist-modify-public",
    "playlist-modify-private",
])

# Shared state between server and main thread
_auth_code = None
_auth_error = None
_server_done = threading.Event()


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Handles the OAuth2 callback from Spotify."""

    def do_GET(self):
        global _auth_code, _auth_error
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        if "code" in params:
            _auth_code = params["code"][0]
            self._respond("Success! You can close this tab and return to the terminal.")
        elif "error" in params:
            _auth_error = params["error"][0]
            self._respond(f"Authorization denied: {_auth_error}")
        else:
            self._respond("Unexpected callback. Check the terminal.")

        _server_done.set()

    def _respond(self, message: str):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = f"<html><body><h2>{message}</h2></body></html>"
        self.wfile.write(html.encode())

    def log_message(self, format, *args):
        pass  # Suppress default HTTP logging


def exchange_code_for_tokens(code: str) -> dict:
    """Exchange authorization code for access + refresh tokens."""
    import urllib.request

    token_url = "https://accounts.spotify.com/api/token"
    credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }).encode()

    req = urllib.request.Request(token_url, data=data, headers={
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET")
        print("  Option A: Create a .env file in this directory")
        print("  Option B: Export them as environment variables")
        return

    # Generate state parameter for CSRF protection
    state = secrets.token_urlsafe(16)

    # Build authorization URL
    auth_params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
    })
    auth_url = f"https://accounts.spotify.com/authorize?{auth_params}"

    # Start local callback server
    server = http.server.HTTPServer(("127.0.0.1", 8888), CallbackHandler)
    server_thread = threading.Thread(target=server.handle_request, daemon=True)
    server_thread.start()

    print("Opening browser for Spotify authorization...")
    print(f"If it doesn't open, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Wait for callback
    _server_done.wait(timeout=120)
    server.server_close()

    if _auth_error:
        print(f"Authorization failed: {_auth_error}")
        return

    if not _auth_code:
        print("Timed out waiting for authorization (2 minutes).")
        return

    print("Authorization code received. Exchanging for tokens...")

    try:
        tokens = exchange_code_for_tokens(_auth_code)
    except Exception as e:
        print(f"Token exchange failed: {e}")
        return

    refresh_token = tokens.get("refresh_token")
    access_token = tokens.get("access_token")

    if not refresh_token:
        print("ERROR: No refresh token in response.")
        print(f"Response: {json.dumps(tokens, indent=2)}")
        return

    print("\n" + "=" * 60)
    print("SUCCESS — Save this to your .env file:")
    print("=" * 60)
    print(f"\nSPOTIFY_REFRESH_TOKEN={refresh_token}")
    print("\n" + "=" * 60)

    # Verify it works
    print(f"\nAccess token received (expires in {tokens.get('expires_in', '?')}s)")
    print(f"Scopes granted: {tokens.get('scope', '?')}")


if __name__ == "__main__":
    main()
