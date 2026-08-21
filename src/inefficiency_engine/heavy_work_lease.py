from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, delete, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore


HEAVY_WORK_LEASE_NAME = "render-heavy-work"
HEAVY_WORK_LEASE_TTL_SECONDS = 3600.0


class HeavyWorkLeaseUnavailable(RuntimeError):
    """Another disposable heavyweight job currently owns the runtime lease."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class HeavyWorkLeaseLedger:
    """Cross-process lease and sequence state for disposable heavyweight jobs.

    The Render parent already runs only one heavy child at a time. This durable lease
    is the second line of defense for manual/restarted invocations: a research process
    and a history process cannot both become authoritative heavy workers. Expiry makes
    a hard-killed process self-healing without weakening any portfolio gate.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.leases = Table(
            "heavy_work_runtime_leases",
            metadata,
            Column("lease_name", String(64), primary_key=True),
            Column("owner", Text, nullable=False),
            Column("acquired_at", Text, nullable=False),
            Column("expires_at", Text, nullable=False),
        )
        self.state = Table(
            "heavy_work_runtime_state",
            metadata,
            Column("job_name", String(64), primary_key=True),
            Column("run_count", Integer, nullable=False),
            Column("updated_at", Text, nullable=False),
        )
        metadata.create_all(store.engine)

    def try_acquire(
        self,
        owner: str,
        *,
        lease_name: str = HEAVY_WORK_LEASE_NAME,
        ttl_seconds: float = HEAVY_WORK_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> bool:
        observed_at = now or _now()
        expires_at = observed_at + timedelta(seconds=max(60.0, float(ttl_seconds)))
        row = {
            "lease_name": lease_name,
            "owner": owner,
            "acquired_at": observed_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        backend = self.store.engine.url.get_backend_name()
        if backend == "postgresql":
            statement = pg_insert(self.leases).values(row).on_conflict_do_update(
                index_elements=[self.leases.c.lease_name],
                set_={
                    "owner": row["owner"],
                    "acquired_at": row["acquired_at"],
                    "expires_at": row["expires_at"],
                },
                where=or_(
                    self.leases.c.expires_at <= observed_at.isoformat(),
                    self.leases.c.owner == owner,
                ),
            )
            with self.store.engine.begin() as db:
                result = db.execute(statement)
            return bool(result.rowcount)
        if backend == "sqlite":
            statement = sqlite_insert(self.leases).values(row).on_conflict_do_update(
                index_elements=[self.leases.c.lease_name],
                set_={
                    "owner": row["owner"],
                    "acquired_at": row["acquired_at"],
                    "expires_at": row["expires_at"],
                },
                where=or_(
                    self.leases.c.expires_at <= observed_at.isoformat(),
                    self.leases.c.owner == owner,
                ),
            )
            with self.store.engine.begin() as db:
                result = db.execute(statement)
            return bool(result.rowcount)

        with self.store.engine.begin() as db:
            current = db.execute(
                select(self.leases.c.owner, self.leases.c.expires_at)
                .where(self.leases.c.lease_name == lease_name)
            ).first()
            if current is not None and current.expires_at > observed_at.isoformat() and current.owner != owner:
                return False
            if current is None:
                db.execute(insert(self.leases), row)
            else:
                db.execute(
                    update(self.leases)
                    .where(self.leases.c.lease_name == lease_name)
                    .values(**row)
                )
        return True

    def release(self, owner: str, *, lease_name: str = HEAVY_WORK_LEASE_NAME) -> None:
        with self.store.engine.begin() as db:
            db.execute(
                delete(self.leases).where(
                    self.leases.c.lease_name == lease_name,
                    self.leases.c.owner == owner,
                )
            )

    def next_sequence(self, job_name: str, *, now: datetime | None = None) -> int:
        observed_at = now or _now()
        with self.store.engine.begin() as db:
            current = db.execute(
                select(self.state.c.run_count)
                .where(self.state.c.job_name == job_name)
                .with_for_update()
            ).scalar_one_or_none()
            next_value = int(current or 0) + 1
            if current is None:
                db.execute(
                    insert(self.state),
                    {
                        "job_name": job_name,
                        "run_count": next_value,
                        "updated_at": observed_at.isoformat(),
                    },
                )
            else:
                db.execute(
                    update(self.state)
                    .where(self.state.c.job_name == job_name)
                    .values(run_count=next_value, updated_at=observed_at.isoformat())
                )
        return next_value

    def current_owner(self, *, lease_name: str = HEAVY_WORK_LEASE_NAME) -> str | None:
        now = _now().isoformat()
        with self.store.engine.connect() as db:
            row = db.execute(
                select(self.leases.c.owner, self.leases.c.expires_at)
                .where(self.leases.c.lease_name == lease_name)
            ).first()
        if row is None or row.expires_at <= now:
            return None
        return str(row.owner)

    @contextmanager
    def lease(
        self,
        owner: str,
        *,
        lease_name: str = HEAVY_WORK_LEASE_NAME,
        ttl_seconds: float = HEAVY_WORK_LEASE_TTL_SECONDS,
    ) -> Iterator[None]:
        if not self.try_acquire(owner, lease_name=lease_name, ttl_seconds=ttl_seconds):
            raise HeavyWorkLeaseUnavailable(f"heavy-work lease is already owned by {self.current_owner()}")
        try:
            yield
        finally:
            self.release(owner, lease_name=lease_name)
