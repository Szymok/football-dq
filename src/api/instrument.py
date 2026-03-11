"""Sentry instrumentation."""
import os
import sentry_sdk

def init_sentry():
    dsn = os.getenv("SENTRY_DSN")
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=1.0,
            profiles_sample_rate=1.0,
        )
    else:
        # Avoid crashing if DSN is missing, useful for local development
        print("SENTRY_DSN not set. Sentry telemetry is disabled.")
