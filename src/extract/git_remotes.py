"""Remote operation helpers with pygit2 feature gating."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

import pygit2

from utils.env_utils import env_bool


@dataclass(frozen=True)
class RemoteFeatureSet:
    """Supported transport features reported by pygit2."""

    ssh: bool
    https: bool


@dataclass(frozen=True)
class RemoteAuthSpec:
    """Remote authentication configuration."""

    username: str | None
    password: str | None
    ssh_pubkey_path: str | None
    ssh_key_path: str | None
    ssh_passphrase: str | None
    allow_invalid_certs: bool = False
    raise_on_push_reject: bool = True


def remote_features() -> RemoteFeatureSet:
    """Return supported remote transport features.

    Returns
    -------
    RemoteFeatureSet
        Supported transport features.
    """
    ssh_flag = getattr(pygit2, "GIT_FEATURE_SSH", 0)
    https_flag = getattr(pygit2, "GIT_FEATURE_HTTPS", 0)
    return RemoteFeatureSet(
        ssh=bool(pygit2.features & ssh_flag),
        https=bool(pygit2.features & https_flag),
    )


def remote_auth_from_env() -> RemoteAuthSpec | None:
    """Return RemoteAuthSpec from environment variables when present.

    Returns
    -------
    RemoteAuthSpec | None
        Resolved authentication spec when any env values are set.
    """
    username = os.getenv("CODEANATOMY_GIT_USERNAME")
    password = os.getenv("CODEANATOMY_GIT_PASSWORD")
    ssh_pubkey_path = os.getenv("CODEANATOMY_GIT_SSH_PUBKEY")
    ssh_key_path = os.getenv("CODEANATOMY_GIT_SSH_KEY")
    ssh_passphrase = os.getenv("CODEANATOMY_GIT_SSH_PASSPHRASE")
    allow_invalid_certs = env_bool("CODEANATOMY_GIT_ALLOW_INVALID_CERTS") or False
    if any(
        (
            username,
            password,
            ssh_pubkey_path,
            ssh_key_path,
            ssh_passphrase,
            allow_invalid_certs,
        )
    ):
        return RemoteAuthSpec(
            username=username,
            password=password,
            ssh_pubkey_path=ssh_pubkey_path,
            ssh_key_path=ssh_key_path,
            ssh_passphrase=ssh_passphrase,
            allow_invalid_certs=allow_invalid_certs,
        )
    return None


def remote_callbacks_from_env() -> pygit2.RemoteCallbacks | None:
    """Return RemoteCallbacks based on environment configuration.

    Returns
    -------
    pygit2.RemoteCallbacks | None
        Callback instance when env configuration is present.
    """
    spec = remote_auth_from_env()
    if spec is None:
        return None
    return RemoteAuthCallbacks(spec)


class RemoteAuthCallbacks(pygit2.RemoteCallbacks):
    """Remote callbacks that negotiate credentials from a spec."""

    def __init__(self, spec: RemoteAuthSpec) -> None:
        super().__init__()
        self._spec = spec

    def credentials(
        self,
        url: str,
        username_from_url: str | None,
        allowed_types: pygit2.enums.CredentialType,
    ) -> pygit2.credentials.Username | pygit2.credentials.UserPass | pygit2.credentials.Keypair:
        """Return credentials based on the allowed types.

        Returns
        -------
        object
            Credential object accepted by pygit2.

        Raises
        ------
        pygit2.GitError
            Raised when no supported credential type is available.
        """
        username = username_from_url or self._spec.username or _username_from_url(url) or "git"
        if allowed_types & pygit2.enums.CredentialType.USERNAME:
            return pygit2.Username(username)
        if allowed_types & pygit2.enums.CredentialType.SSH_KEY:
            if self._spec.ssh_key_path and self._spec.ssh_pubkey_path:
                return pygit2.Keypair(
                    username,
                    self._spec.ssh_pubkey_path,
                    self._spec.ssh_key_path,
                    self._spec.ssh_passphrase or "",
                )
            return pygit2.KeypairFromAgent(username)
        if (
            allowed_types & pygit2.enums.CredentialType.USERPASS_PLAINTEXT
            and self._spec.username
            and self._spec.password
        ):
            return pygit2.UserPass(self._spec.username, self._spec.password)
        msg = "No supported credentials available for remote"
        raise pygit2.GitError(msg)

    def certificate_check(self, certificate: None, valid: int, host: bytes) -> bool:
        """Return whether to accept a certificate check result.

        Returns
        -------
        bool
            ``True`` to accept the certificate, otherwise ``False``.
        """
        if valid:
            return True
        if not host:
            return False
        if certificate is None:
            return self._spec.allow_invalid_certs
        return self._spec.allow_invalid_certs

    def push_update_reference(self, refname: str, message: str | None) -> None:
        """Handle per-ref push update responses.

        Raises
        ------
        pygit2.GitError
            Raised when the remote rejects the ref update.
        """
        if message and self._spec.raise_on_push_reject:
            msg = f"push rejected for {refname}: {message}"
            raise pygit2.GitError(msg)


def fetch_remote(
    repo: pygit2.Repository,
    *,
    name: str = "origin",
    callbacks: pygit2.RemoteCallbacks | None = None,
    prune: bool = True,
    depth: int | None = None,
) -> bool:
    """Fetch a remote, returning True on success.

    Returns
    -------
    bool
        ``True`` when the fetch succeeded.
    """
    try:
        remote = repo.remotes[name]
    except KeyError:
        return False
    try:
        if depth is None:
            remote.fetch(
                callbacks=callbacks,
                prune=pygit2.enums.FetchPrune.PRUNE
                if prune
                else pygit2.enums.FetchPrune.UNSPECIFIED,
            )
        else:
            remote.fetch(
                callbacks=callbacks,
                prune=pygit2.enums.FetchPrune.PRUNE
                if prune
                else pygit2.enums.FetchPrune.UNSPECIFIED,
                depth=depth,
            )
    except pygit2.GitError:
        return False
    return True


def _username_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.username:
        return parsed.username
    if "@" in url and ":" in url:
        user_host = url.split(":", 1)[0]
        return user_host.split("@")[0] or None
    return None


__all__ = [
    "RemoteAuthCallbacks",
    "RemoteAuthSpec",
    "RemoteFeatureSet",
    "fetch_remote",
    "remote_auth_from_env",
    "remote_callbacks_from_env",
    "remote_features",
]
