from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

import sqlexplore.app as app_module


class _FakeHttpResponse:
    def __init__(self, payload: bytes) -> None:
        self._buffer = BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
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

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)

    download_remote = getattr(app_module, "_download_remote_parquet")
    out_path = download_remote(url, tmp_path)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert f"remote={url}" in out
    assert f"local={out_path}" in out
    assert "[download] downloaded=" in out
    assert "[download] complete" in out


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
    assert "[download] complete" in out


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

    def fake_download(url: str, download_dir: Path, overwrite: bool = False) -> Path:
        captured["download_url"] = url
        captured["download_dir"] = download_dir
        captured["overwrite"] = overwrite
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
    assert captured["data_path"] == downloaded
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert captured["closed"] is True


def test_main_passes_overwrite_flag_for_remote_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    remote_url = "https://example.com/data.parquet"
    downloaded = tmp_path / "from-remote.parquet"
    downloaded.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fake_download(url: str, download_dir: Path, overwrite: bool = False) -> Path:
        captured["download_url"] = url
        captured["download_dir"] = download_dir
        captured["overwrite"] = overwrite
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
    assert captured["data_path"] == downloaded
    assert captured["closed"] is True


def test_main_keeps_local_path_behavior(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    local_path = tmp_path / "sample.parquet"
    local_path.write_bytes(b"PAR1")
    captured: dict[str, Any] = {}

    def fail_download(url: str, download_dir: Path) -> Path:
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
