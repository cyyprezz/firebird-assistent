from __future__ import annotations

import json
import os
from typing import Dict, Optional

from pydantic import BaseModel, Field


APP_DIR_NAME = "firebird-assistant"
STORE_FILENAME = "profiles.json"


class ConnectionProfile(BaseModel):
    name: str = Field(..., description="Profile name")
    dsn: str = Field(..., description="Firebird DSN (host[:/port]:path or local path)")
    user: Optional[str] = Field(None, description="Username (no password persisted)")
    role: Optional[str] = Field(None, description="Optional role")
    charset: str = Field("UTF8", description="Character set")


class ProfileStore(BaseModel):
    profiles: Dict[str, ConnectionProfile] = Field(default_factory=dict)

    def add(self, profile: ConnectionProfile) -> None:
        self.profiles[profile.name] = profile

    def remove(self, name: str) -> None:
        if name in self.profiles:
            del self.profiles[name]

    def rename(self, old: str, new: str) -> None:
        if old not in self.profiles:
            raise KeyError(old)
        if new in self.profiles:
            raise ValueError(f"Profile exists: {new}")
        prof = self.profiles.pop(old)
        prof = prof.copy(update={"name": new})
        self.profiles[new] = prof


def _config_dir() -> str:
    # Windows
    if os.name == "nt":
        base = os.environ.get("APPDATA") or os.path.expanduser("~\\AppData\\Roaming")
        return os.path.join(base, APP_DIR_NAME)
    # Unix-like
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = xdg if xdg else os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, APP_DIR_NAME)


def _store_path() -> str:
    return os.path.join(_config_dir(), STORE_FILENAME)


def load_store() -> ProfileStore:
    path = _store_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return ProfileStore.model_validate(data)
    except FileNotFoundError:
        return ProfileStore()


def save_store(store: ProfileStore) -> None:
    cfgdir = _config_dir()
    os.makedirs(cfgdir, exist_ok=True)
    path = _store_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store.model_dump(), f, ensure_ascii=False, indent=2)


def get_profile(name: str) -> Optional[ConnectionProfile]:
    store = load_store()
    return store.profiles.get(name)


def list_profiles() -> Dict[str, ConnectionProfile]:
    return load_store().profiles

