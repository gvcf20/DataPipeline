from __future__ import annotations

try:
    from .pipeline import main
except ImportError:
    from pipeline import main


def create_csv() -> None:
    main([])


if __name__ == "__main__":
    create_csv()
