import logging
import os


def configure_logging() -> None:
    """Configure application-wide logging.

    - Avoids duplicate handler installation.
    - Uses Cloud Run friendly format on stdout.
    - Level can be controlled via LOG_LEVEL env var.
    """

    root = logging.getLogger()
    if root.handlers:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root.setLevel(level)
    root.addHandler(handler)

    # Quiet overly chatty libraries unless explicitly overridden.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
