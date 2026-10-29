"""Позволяет запускать ``python -m scanner`` наравне с ``python -m scanner.main``."""

from .main import main

if __name__ == "__main__":
    raise SystemExit(main())
