import logging


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper())
    )

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("API.DB.CLIENT").setLevel(logging.WARNING)
    logging.getLogger("API.DB.REPOSITORY").setLevel(logging.INFO)
