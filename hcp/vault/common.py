import os

from elastic.pipes.core.util import fatal


def update_url_token_from_env(url, token, log):
    if url is None:
        log.debug("reading url from environment 'VAULT_ADDR'")
        url = os.environ.get("VAULT_ADDR") or None
    if token is None:
        log.debug("reading token from environment 'VAULT_TOKEN'")
        token = os.environ.get("VAULT_TOKEN") or None

    if not url:
        fatal("vault url is not defined")
    if not token:
        fatal("vault token is not defined")

    return url, token
