"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Superset Application Configuration & Basemap Security Policy
============================================================
Configures:
  1. MAPBOX_API_KEY (from environment if provided by user)
  2. TALISMAN_CONFIG with relaxed Content Security Policy (CSP)
     allowing CartoDB (Positron / Dark Matter / Voyager), OpenStreetMap,
     and Mapbox CDN tiles, styles, fonts, and Web Workers.
  3. Feature flags and custom basemap settings.
"""

from __future__ import annotations

import os

# -----------------------------------------------------------------------------
# 1. MAPBOX API KEY (Optional fallback for Mapbox vector tiles)
# -----------------------------------------------------------------------------
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# -----------------------------------------------------------------------------
# 2. CONTENT SECURITY POLICY (CSP) FOR DECK.GL & BASEMAP TILES
# -----------------------------------------------------------------------------
# Talisman is enabled by default in Superset. We must explicitly whitelist
# CartoDB, OpenStreetMap, and Mapbox domains in connect-src, img-src, and worker-src
# so that the browser does not block map background tile and style requests.
TALISMAN_ENABLED = True

TALISMAN_CONFIG = {
    "content_security_policy": {
        "base-uri": ["'self'"],
        "default-src": ["'self'"],
        "img-src": [
            "'self'",
            "blob:",
            "data:",
            "https://*.cartocdn.com",
            "https://*.basemaps.cartocdn.com",
            "https://basemaps.cartocdn.com",
            "https://*.tile.openstreetmap.org",
            "https://tile.openstreetmap.org",
            "https://api.mapbox.com",
            "https://*.mapbox.com",
        ],
        "worker-src": [
            "'self'",
            "blob:",
            "data:",
        ],
        "connect-src": [
            "'self'",
            "https://api.mapbox.com",
            "https://events.mapbox.com",
            "https://*.cartocdn.com",
            "https://*.basemaps.cartocdn.com",
            "https://basemaps.cartocdn.com",
            "https://tiles.basemaps.cartocdn.com",
            "https://*.tile.openstreetmap.org",
            "https://tile.openstreetmap.org",
        ],
        "object-src": "'none'",
        "style-src": [
            "'self'",
            "'unsafe-inline'",
        ],
        "script-src": [
            "'self'",
            "'strict-dynamic'",
        ],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "session_cookie_secure": False,
}

TALISMAN_DEV_CONFIG = TALISMAN_CONFIG
