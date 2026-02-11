from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

import sqlexplore.app as app_module


class _FakeHttpResponse:
    def __init__(
        self,
        payload: bytes,
        *,
        read_chunk_size: int | None = None,
        content_length: int | None = None,
    ) -> None:
        self._buffer = BytesIO(payload)
        self._read_chunk_size = read_chunk_size
        self.headers: dict[str, str] = {}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def read(self, size: int = -1) -> bytes:
        if size >= 0 and self._read_chunk_size is not None:
            size = min(size, self._read_chunk_size)
        return self._buffer.read(size)

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def test_download_remote_parquet_writes_file_and_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    payload = b"PAR1....test-bytes...."
    url = "https://example.com/data.parquet"
    startup_activity_messages: list[str] = []

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)

    download_remote = getattr(app_module, "_download_remote_parquet")
    out_path = download_remote(url, tmp_path, activity_messages=startup_activity_messages)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert f"remote={url}" in out
    assert f"local={out_path}" in out
    assert "[download] Complete" in out
    assert startup_activity_messages == [line for line in out.splitlines() if line]


def test_download_remote_parquet_does_not_log_progress_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    payload = b"1234567890"
    url = "https://example.com/data.parquet"
    startup_activity_messages: list[str] = []

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload, read_chunk_size=5, content_length=len(payload))

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)

    download_remote = getattr(app_module, "_download_remote_parquet")
    out_path = download_remote(url, tmp_path, activity_messages=startup_activity_messages)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert all(not line.startswith("[download] progress=") for line in startup_activity_messages)
    assert "[download] progress=" not in out


def test_download_remote_parquet_refuses_overwrite(tmp_path: Path, capsys: Any) -> None:
    existing = tmp_path / "data.parquet"
    existing.write_bytes(b"PAR1")
    download_remote = getattr(app_module, "_download_remote_parquet")
    with pytest.raises(typer.Exit) as out:
        download_remote("https://example.com/data.parquet", tmp_path)
    assert out.value.exit_code == 1
    err = capsys.readouterr().err
    assert "stopping download" in err
    assert "--overwrite" in err
    assert f"{existing.name}" in err


def test_download_remote_parquet_overwrites_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    existing = tmp_path / "data.parquet"
    existing.write_bytes(b"old")
    payload = b"new-data"
    url = "https://example.com/data.parquet"

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)
    download_remote = getattr(app_module, "_download_remote_parquet")
    out_path = download_remote(url, tmp_path, overwrite=True)

    assert out_path == existing.resolve()
    assert existing.read_bytes() == payload
    out = capsys.readouterr().out
    assert "[download] Complete" in out


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://example.com/data.csv", "data.csv"),
        ("https://example.com/data.tsv", "data.tsv"),
    ],
)
def test_remote_filename_accepts_csv_and_tsv(url: str, expected: str) -> None:
    remote_filename = getattr(app_module, "_remote_filename")
    assert remote_filename(url) == expected


def test_resolve_data_path_rejects_remote_unsupported_extension() -> None:
    resolve_data_path = getattr(app_module, "_resolve_data_path")
    with pytest.raises(typer.BadParameter, match=r"Remote URL must end with \.csv, \.tsv, \.parquet, or \.pq\."):
        resolve_data_path("https://example.com/data.json")


def test_main_uses_downloaded_path_when_data_arg_is_https(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    remote_url = "https://example.com/data.parquet"
    downloaded = tmp_path / "from-remote.parquet"
    downloaded.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fake_download(
        url: str,
        download_dir: Path,
        overwrite: bool = False,
        activity_messages: list[str] | None = None,
    ) -> Path:
        captured["download_url"] = url
        captured["download_dir"] = download_dir
        captured["overwrite"] = overwrite
        captured["activity_messages"] = activity_messages
        return downloaded

    class FakeEngine:
        def __init__(
            self,
            data_path: Path,
            table_name: str,
            database: str,
            default_limit: int,
            max_rows_display: int,
            max_value_chars: int,
        ) -> None:
            captured["data_path"] = data_path
            captured["table_name"] = table_name
            captured["database"] = database
            captured["default_limit"] = default_limit
            captured["max_rows_display"] = max_rows_display
            captured["max_value_chars"] = max_value_chars
            self.default_query = 'SELECT * FROM "data" LIMIT 100'
            self.max_value_chars = max_value_chars

        def run_sql(self, sql_text: str, remember: bool = True) -> app_module.EngineResponse:
            captured["run_sql"] = (sql_text, remember)
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module, "_download_remote_parquet", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [remote_url, "--no-ui"])

    assert result.exit_code == 0
    assert captured["download_url"] == remote_url
    assert captured["download_dir"] == Path("data/downloads")
    assert captured["overwrite"] is False
    assert isinstance(captured["activity_messages"], list)
    assert captured["data_path"] == downloaded
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert captured["closed"] is True


def test_main_passes_overwrite_flag_for_remote_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    remote_url = "https://example.com/data.parquet"
    downloaded = tmp_path / "from-remote.parquet"
    downloaded.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fake_download(
        url: str,
        download_dir: Path,
        overwrite: bool = False,
        activity_messages: list[str] | None = None,
    ) -> Path:
        captured["download_url"] = url
        captured["download_dir"] = download_dir
        captured["overwrite"] = overwrite
        captured["activity_messages"] = activity_messages
        return downloaded

    class FakeEngine:
        def __init__(
            self,
            data_path: Path,
            table_name: str,
            database: str,
            default_limit: int,
            max_rows_display: int,
            max_value_chars: int,
        ) -> None:
            captured["data_path"] = data_path
            self.default_query = 'SELECT * FROM "data" LIMIT 100'
            self.max_value_chars = max_value_chars

        def run_sql(self, sql_text: str, remember: bool = True) -> app_module.EngineResponse:
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module, "_download_remote_parquet", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [remote_url, "--overwrite", "--no-ui"])

    assert result.exit_code == 0
    assert captured["download_url"] == remote_url
    assert captured["download_dir"] == Path("data/downloads")
    assert captured["overwrite"] is True
    assert isinstance(captured["activity_messages"], list)
    assert captured["data_path"] == downloaded
    assert captured["closed"] is True


def test_main_passes_download_logs_to_tui_on_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    remote_url = "https://example.com/data.parquet"
    downloaded = tmp_path / "from-remote.parquet"
    downloaded.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fake_download(
        url: str,
        download_dir: Path,
        overwrite: bool = False,
        activity_messages: list[str] | None = None,
    ) -> Path:
        assert activity_messages is not None
        activity_messages.extend(
            [
                f"[download] remote={url}",
                f"[download] local={downloaded}",
                "[download] Complete elapsed=0.001s size=4 B (4 bytes)",
            ]
        )
        return downloaded

    class FakeEngine:
        def __init__(
            self,
            data_path: Path,
            table_name: str,
            database: str,
            default_limit: int,
            max_rows_display: int,
            max_value_chars: int,
        ) -> None:
            captured["data_path"] = data_path

        def close(self) -> None:
            captured["closed"] = True

    class FakeTui:
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            captured["engine"] = engine
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["run"] = True

    monkeypatch.setattr(app_module, "_download_remote_parquet", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, [remote_url])

    assert result.exit_code == 0
    assert captured["data_path"] == downloaded
    assert captured["run"] is True
    assert captured["startup_activity_messages"] == [
        f"[download] remote={remote_url}",
        f"[download] local={downloaded}",
        "[download] Complete elapsed=0.001s size=4 B (4 bytes)",
    ]
    assert captured["closed"] is True


def test_main_keeps_local_path_behavior(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    local_path = tmp_path / "sample.parquet"
    local_path.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fail_download(
        url: str,
        download_dir: Path,
        overwrite: bool = False,
        activity_messages: list[str] | None = None,
    ) -> Path:
        raise AssertionError("download should not be called for local file path")

    class FakeEngine:
        def __init__(
            self,
            data_path: Path,
            table_name: str,
            database: str,
            default_limit: int,
            max_rows_display: int,
            max_value_chars: int,
        ) -> None:
            captured["data_path"] = data_path
            self.default_query = 'SELECT * FROM "data" LIMIT 100'
            self.max_value_chars = max_value_chars

        def run_sql(self, sql_text: str, remember: bool = True) -> app_module.EngineResponse:
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module, "_download_remote_parquet", fail_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [str(local_path), "--no-ui"])

    assert result.exit_code == 0
    assert captured["data_path"] == local_path.resolve()
    assert captured["closed"] is True
