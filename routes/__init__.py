"""
routes/__init__.py
------------------------------------------------------------
Marks `routes` as a package.

We keep this file intentionally minimal to avoid import side-effects
during startup on Render (where any failing import can break router mounts).
"""
