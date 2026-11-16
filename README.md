# Prototype playtests

Web (Unity WebGL) playtest builds of the Gravity prototypes, served by GitHub Pages.
Source code lives in the prototypes repo; this repo only holds built files.

- Site: https://bgs-daniel.github.io/prototypePlaytests/
- One folder per game: `/<game>/` (e.g. `/turf-trade/`). Each deploy replaces that folder only.
- Add a line for your game to `index.html` when you first deploy it.
- Build settings: WebGL, compression **Disabled** (or Brotli/Gzip **with Decompression Fallback**), because Pages doesn't send `Content-Encoding` headers.
- Never put keys or secrets in a build. This site is public to anyone with the link.
