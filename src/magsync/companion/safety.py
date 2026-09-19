"""Conservative public metadata boundary; no provider URLs or local paths."""
import re

from magsync.core.diagnostics import sanitize_external_error


def public_text(value: str, maximum: int = 512) -> str:
    value = re.sub(r'https?://[^\s<>]+', '[source reference]', value, flags=re.I)
    value = re.sub(r'(?<!\w)(?:/|[A-Za-z]:\\)[^\s,;]+', '[path]', value)
    return sanitize_external_error(value, maximum)


def configure_service_logging():
    """Apply the same conservative metadata boundary to service log sinks."""
    import logging

    class PublicFilter(logging.Filter):
        def filter(self, record):
            try:
                record.msg = public_text(record.getMessage(), 2000)
            except Exception:
                record.msg = 'Unable to render service diagnostic'
            record.args = ()
            record.exc_info = record.exc_text = None
            return True

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    for handler in logging.getLogger().handlers:
        handler.addFilter(PublicFilter())
