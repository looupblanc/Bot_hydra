from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from hydra.data.multitimeframe import resample_closed_bars
from hydra.propfirm.trading_day import trading_day_for_timestamp
from hydra.shadow.contract_resolver import ContractResolution


UTC = timezone.utc
CHICAGO = ZoneInfo("America/Chicago")
STORE_SCHEMA = "hydra_forward_bar_store_v1"


class ForwardBarIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class MarketClosure:
    start_at_utc: datetime
    end_at_utc: datetime
    reason: str

    def __post_init__(self) -> None:
        start = _aware_utc(self.start_at_utc)
        end = _aware_utc(self.end_at_utc)
        if start >= end or not self.reason:
            raise ValueError("A market closure needs an ordered interval and reason.")


@dataclass(frozen=True)
class CmeSessionCalendar:
    version: str
    holiday_schedule_through: date | None
    closures: tuple[MarketClosure, ...] = ()

    @property
    def holiday_coverage_verified(self) -> bool:
        return self.holiday_schedule_through is not None

    def covers(self, at: datetime) -> bool:
        return bool(
            self.holiday_schedule_through
            and _aware_utc(at).astimezone(CHICAGO).date() <= self.holiday_schedule_through
        )

    def market_state(self, at: datetime) -> str:
        current = _aware_utc(at)
        for closure in self.closures:
            if _aware_utc(closure.start_at_utc) <= current < _aware_utc(closure.end_at_utc):
                return "HOLIDAY_CLOSED"
        local = current.astimezone(CHICAGO)
        weekday = local.weekday()
        minute = local.hour * 60 + local.minute
        if weekday == 5 or (weekday == 6 and minute < 17 * 60):
            return "WEEKEND_CLOSED"
        if weekday == 4 and minute >= 16 * 60:
            return "WEEKEND_CLOSED"
        if 16 * 60 <= minute < 17 * 60:
            return "MAINTENANCE_CLOSED"
        return "OPEN"

    @classmethod
    def weekly_only(cls) -> "CmeSessionCalendar":
        return cls(
            version="cme_weekly_session_dst_v1_unverified_holidays",
            holiday_schedule_through=None,
        )


@dataclass(frozen=True)
class ForwardBar:
    source_id: str
    root: str
    contract: str
    timeframe: str
    bar_start_at_utc: datetime
    bar_close_at_utc: datetime
    availability_at_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_sequence: int

    @property
    def payload_hash(self) -> str:
        raw = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("bar_start_at_utc", "bar_close_at_utc", "availability_at_utc"):
            payload[key] = _aware_utc(payload[key]).isoformat()
        return payload

    def validate(self, *, observed_at: datetime, calendar: CmeSessionCalendar) -> None:
        start = _aware_utc(self.bar_start_at_utc)
        close = _aware_utc(self.bar_close_at_utc)
        available = _aware_utc(self.availability_at_utc)
        observed = _aware_utc(observed_at)
        if not self.source_id or not self.root or not self.contract:
            raise ForwardBarIntegrityError("Source, root and explicit contract are required.")
        if self.timeframe != "1m" or close - start != pd.Timedelta(minutes=1):
            raise ForwardBarIntegrityError("The canonical forward store accepts closed 1m bars only.")
        if available < close or available > observed:
            raise ForwardBarIntegrityError("Incomplete or future bar availability is prohibited.")
        if start > observed or self.source_sequence <= 0:
            raise ForwardBarIntegrityError("Future timestamps or non-positive sequences are prohibited.")
        prices = (self.open, self.high, self.low, self.close)
        if not all(math.isfinite(float(value)) and float(value) > 0 for value in prices):
            raise ForwardBarIntegrityError("OHLC prices must be finite and positive.")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ForwardBarIntegrityError("OHLC envelope is invalid.")
        if self.low > self.high or not math.isfinite(float(self.volume)) or self.volume < 0:
            raise ForwardBarIntegrityError("Range or volume is invalid.")
        if calendar.market_state(start) != "OPEN":
            raise ForwardBarIntegrityError("A bar cannot start while the CME session is closed.")


class ForwardBarStore:
    """Restart-safe SQLite store with a process-wide, non-blocking writer lock."""

    def __init__(
        self,
        path: str | Path,
        *,
        calendar: CmeSessionCalendar | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.calendar = calendar or CmeSessionCalendar.weekly_only()

    @property
    def exists(self) -> bool:
        return self.path.is_file()

    @contextmanager
    def writer(self, *, writer_id: str) -> Iterator["ForwardBarWriter"]:
        if not writer_id:
            raise ValueError("A stable writer identity is required.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another forward-bar writer already holds the lock.") from exc
            lock.seek(0)
            lock.truncate()
            lock.write(f"{os.getpid()}:{writer_id}")
            lock.flush()
            connection = sqlite3.connect(self.path)
            try:
                _ensure_schema(connection)
                yield ForwardBarWriter(self, connection, writer_id)
            finally:
                connection.close()
                fcntl.flock(lock, fcntl.LOCK_UN)

    def summary(self) -> dict[str, Any]:
        if not self.exists:
            return {
                "schema": STORE_SCHEMA,
                "exists": False,
                "bar_count": 0,
                "missing_bar_count": 0,
                "latest_completed_bar_at_utc": None,
            }
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            bar_count = int(connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0])
            missing = int(
                connection.execute(
                    "SELECT COALESCE(SUM(missing_count), 0) FROM missing_intervals"
                ).fetchone()[0]
            )
            latest = connection.execute(
                "SELECT MAX(bar_close_at_utc) FROM bars"
            ).fetchone()[0]
            duplicates = int(
                connection.execute(
                    "SELECT COUNT(*) FROM ingestion_audit WHERE status='DUPLICATE_IGNORED'"
                ).fetchone()[0]
            )
            conflicts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM ingestion_audit WHERE status LIKE '%REJECTED%'"
                ).fetchone()[0]
            )
            return {
                "schema": STORE_SCHEMA,
                "exists": True,
                "sqlite_integrity": integrity,
                "bar_count": bar_count,
                "missing_bar_count": missing,
                "duplicate_bar_count": duplicates,
                "rejected_bar_count": conflicts,
                "latest_completed_bar_at_utc": latest,
                "calendar_version": self.calendar.version,
                "holiday_schedule_through": (
                    self.calendar.holiday_schedule_through.isoformat()
                    if self.calendar.holiday_schedule_through
                    else None
                ),
            }
        finally:
            connection.close()

    def latest_by_root(self) -> dict[str, dict[str, Any]]:
        if not self.exists:
            return {}
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                """
                SELECT b.* FROM bars b
                JOIN (
                    SELECT root, MAX(bar_close_at_utc) AS latest
                    FROM bars GROUP BY root
                ) x ON b.root=x.root AND b.bar_close_at_utc=x.latest
                ORDER BY b.root
                """
            ).fetchall()
            return {str(row["root"]): dict(row) for row in rows}
        finally:
            connection.close()

    def closed_multitimeframe(
        self,
        *,
        root: str,
        contract: str,
        minutes: int,
        as_of: datetime,
    ) -> pd.DataFrame:
        if minutes not in {5, 15, 30, 60}:
            raise ValueError("Forward MTF supports 5m, 15m, 30m and 60m.")
        if not self.exists:
            return pd.DataFrame()
        cutoff = _aware_utc(as_of)
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        try:
            frame = pd.read_sql_query(
                """
                SELECT root AS symbol, contract AS active_contract,
                       bar_start_at_utc AS timestamp, open, high, low, close, volume
                FROM bars
                WHERE root=? AND contract=? AND timeframe='1m'
                  AND availability_at_utc<=?
                ORDER BY bar_start_at_utc
                """,
                connection,
                params=(root, contract, cutoff.isoformat()),
            )
        finally:
            connection.close()
        if frame.empty:
            return frame
        output = resample_closed_bars(frame, minutes, as_of=cutoff)
        complete = (
            output["source_row_count"].eq(minutes)
            & output["source_last_timestamp"].eq(
                output["source_bar_close"] - pd.Timedelta(minutes=1)
            )
        )
        return output[complete].reset_index(drop=True)


class ForwardBarWriter:
    def __init__(
        self,
        store: ForwardBarStore,
        connection: sqlite3.Connection,
        writer_id: str,
    ) -> None:
        self.store = store
        self.connection = connection
        self.writer_id = writer_id

    def append(
        self,
        bar: ForwardBar,
        *,
        observed_at: datetime,
        resolution: ContractResolution,
    ) -> dict[str, Any]:
        observed = _aware_utc(observed_at)
        try:
            bar.validate(observed_at=observed, calendar=self.store.calendar)
            expected = resolution.contract_for(bar.root)
            if expected.contract != bar.contract:
                raise ForwardBarIntegrityError(
                    f"Bar contract {bar.contract} differs from resolved {expected.contract}."
                )
        except Exception as exc:
            self._audit(bar, observed, "VALIDATION_REJECTED", str(exc))
            raise

        identity = (
            bar.source_id,
            bar.root,
            bar.contract,
            bar.timeframe,
            _aware_utc(bar.bar_start_at_utc).isoformat(),
        )
        existing = self.connection.execute(
            """
            SELECT payload_hash FROM bars
            WHERE source_id=? AND root=? AND contract=? AND timeframe=?
              AND bar_start_at_utc=?
            """,
            identity,
        ).fetchone()
        if existing:
            if existing[0] == bar.payload_hash:
                self._audit(bar, observed, "DUPLICATE_IGNORED", "exact_payload")
                return {"status": "DUPLICATE_IGNORED", "payload_hash": bar.payload_hash}
            self._audit(bar, observed, "CONFLICT_REJECTED", "divergent_duplicate")
            raise ForwardBarIntegrityError("Divergent duplicate bar rejected.")

        last = self.connection.execute(
            """
            SELECT bar_start_at_utc, bar_close_at_utc, source_sequence
            FROM bars WHERE source_id=? AND root=? AND contract=? AND timeframe=?
            ORDER BY source_sequence DESC LIMIT 1
            """,
            identity[:4],
        ).fetchone()
        missing_count = 0
        if last:
            last_start = _aware_utc(last[0])
            last_close = _aware_utc(last[1])
            if bar.source_sequence <= int(last[2]) or _aware_utc(bar.bar_start_at_utc) <= last_start:
                self._audit(bar, observed, "MONOTONICITY_REJECTED", "non_monotonic_sequence_or_time")
                raise ForwardBarIntegrityError("Source time and sequence must be strictly monotonic.")
            same_session = (
                trading_day_for_timestamp(pd.Timestamp(last_start)).trading_day
                == trading_day_for_timestamp(pd.Timestamp(bar.bar_start_at_utc)).trading_day
            )
            gap = _aware_utc(bar.bar_start_at_utc) - last_close
            if same_session and gap > pd.Timedelta(0):
                missing_count = int(gap / pd.Timedelta(minutes=1))

        payload = bar.to_dict()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO bars(
                    source_id, root, contract, timeframe, bar_start_at_utc,
                    bar_close_at_utc, availability_at_utc, open, high, low, close,
                    volume, source_sequence, payload_hash, trading_day
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    bar.source_id,
                    bar.root,
                    bar.contract,
                    bar.timeframe,
                    payload["bar_start_at_utc"],
                    payload["bar_close_at_utc"],
                    payload["availability_at_utc"],
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.source_sequence,
                    bar.payload_hash,
                    trading_day_for_timestamp(pd.Timestamp(bar.bar_start_at_utc)).trading_day,
                ),
            )
            if missing_count:
                self.connection.execute(
                    """
                    INSERT INTO missing_intervals(
                        source_id, root, contract, timeframe, after_close_at_utc,
                        before_start_at_utc, missing_count, detected_at_utc
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        bar.source_id,
                        bar.root,
                        bar.contract,
                        bar.timeframe,
                        last[1],
                        payload["bar_start_at_utc"],
                        missing_count,
                        observed.isoformat(),
                    ),
                )
            self._audit(bar, observed, "ACCEPTED", f"missing_before={missing_count}", commit=False)
        return {
            "status": "ACCEPTED",
            "payload_hash": bar.payload_hash,
            "missing_before": missing_count,
        }

    def _audit(
        self,
        bar: ForwardBar,
        observed: datetime,
        status: str,
        detail: str,
        *,
        commit: bool = True,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO ingestion_audit(
                observed_at_utc, writer_id, source_id, root, contract,
                bar_start_at_utc, source_sequence, payload_hash, status, detail
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                observed.isoformat(),
                self.writer_id,
                bar.source_id,
                bar.root,
                bar.contract,
                _aware_utc(bar.bar_start_at_utc).isoformat(),
                bar.source_sequence,
                bar.payload_hash,
                status,
                detail,
            ),
        )
        if commit:
            self.connection.commit()


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bars(
            source_id TEXT NOT NULL,
            root TEXT NOT NULL,
            contract TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bar_start_at_utc TEXT NOT NULL,
            bar_close_at_utc TEXT NOT NULL,
            availability_at_utc TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            source_sequence INTEGER NOT NULL,
            payload_hash TEXT NOT NULL,
            trading_day TEXT NOT NULL,
            PRIMARY KEY(source_id, root, contract, timeframe, bar_start_at_utc),
            UNIQUE(source_id, root, contract, timeframe, source_sequence)
        );
        CREATE TABLE IF NOT EXISTS missing_intervals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            root TEXT NOT NULL,
            contract TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            after_close_at_utc TEXT NOT NULL,
            before_start_at_utc TEXT NOT NULL,
            missing_count INTEGER NOT NULL,
            detected_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingestion_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at_utc TEXT NOT NULL,
            writer_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            root TEXT NOT NULL,
            contract TEXT NOT NULL,
            bar_start_at_utc TEXT NOT NULL,
            source_sequence INTEGER NOT NULL,
            payload_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL
        );
        """
    )
    current = connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()
    if current and current[0] != STORE_SCHEMA:
        raise ForwardBarIntegrityError(f"Unsupported store schema: {current[0]}")
    connection.execute(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES ('schema',?)", (STORE_SCHEMA,)
    )
    connection.commit()


def _aware_utc(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ForwardBarIntegrityError("Naive timestamps are prohibited in the forward feed.")
    return parsed.astimezone(UTC)
