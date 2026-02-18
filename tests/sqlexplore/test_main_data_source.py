import re
from contextlib import contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

import sqlexplore.app as app_module
import sqlexplore.cli.data_paths as data_paths_module
import sqlexplore.core.engine as engine_module

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _strip_ansi(value: str) -> str:
    return _ANSI_ESCAPE_RE.sub("", value)


def _expected_default_download_dir() -> Path:
    return (Path(typer.get_app_dir("sqlexplore")) / "downloads").resolve()


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


class _ExplodingHttpResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)
        self._reads = 0
        self.headers: dict[str, str] = {"Content-Length": str(sum(len(chunk) for chunk in chunks))}

    def read(self, _size: int = -1) -> bytes:
        if self._reads < len(self._chunks):
            chunk = self._chunks[self._reads]
            self._reads += 1
            return chunk
        raise RuntimeError("stream read failed")

    def __enter__(self) -> "_ExplodingHttpResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def test_format_byte_count_scales_kb_mb_and_gb() -> None:
    assert data_paths_module.format_byte_count(1024) == "1.0 KB"
    assert data_paths_module.format_byte_count(1024 * 1024) == "1.0 MB"
    assert data_paths_module.format_byte_count(1024 * 1024 * 1024) == "1.0 GB"


def test_remote_filename_defaults_to_sanitized_host_when_path_is_empty() -> None:
    filename = data_paths_module.remote_filename("https://example.com:8443")
    assert re.fullmatch(r"example\.com-8443-[0-9a-f]{12}\.parquet", filename)


def test_remote_filename_distinguishes_same_basename_from_different_urls() -> None:
    first = data_paths_module.remote_filename("https://example.com/a/test-00000-of-00001.parquet")
    second = data_paths_module.remote_filename("https://another.example/b/test-00000-of-00001.parquet")
    assert first != second
    assert first.endswith(".parquet")
    assert second.endswith(".parquet")


def test_emit_download_log_can_skip_activity_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, bool]] = []

    def fake_echo(message: str, err: bool = False) -> None:
        calls.append((message, err))

    monkeypatch.setattr(data_paths_module.typer, "echo", fake_echo)
    messages: list[str] = []
    data_paths_module.emit_download_log("hello", messages, echo=True, include_activity=False)

    assert calls == [("hello", False)]
    assert messages == []


def test_remote_content_length_handles_missing_and_invalid_headers() -> None:
    class _NoHeaders:
        headers: Any = None

    assert data_paths_module.remote_content_length(_NoHeaders()) is None
    assert data_paths_module.remote_content_length(type("R", (), {"headers": {"Content-Length": "bad"}})()) is None
    assert data_paths_module.remote_content_length(type("R", (), {"headers": {"Content-Length": "0"}})()) is None
    assert data_paths_module.remote_content_length(type("R", (), {"headers": {"Content-Length": "7"}})()) == 7


def test_ensure_download_dir_wraps_mkdir_oserror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "downloads"
    real_mkdir = Path.mkdir

    def failing_mkdir(self: Path, parents: bool = False, exist_ok: bool = False) -> None:
        if self == target:
            raise OSError("permission denied")
        real_mkdir(self, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", failing_mkdir)
    with pytest.raises(typer.BadParameter, match=r"Download directory is not writable"):
        data_paths_module.ensure_download_dir(target)


def test_download_remote_data_file_writes_file_and_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    payload = b"PAR1....test-bytes...."
    url = "https://example.com/data.parquet"
    startup_activity_messages: list[str] = []

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(data_paths_module, "urlopen", fake_urlopen)
    out_path = data_paths_module.download_remote_data_file(url, tmp_path, activity_messages=startup_activity_messages)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert f"remote={url}" in out
    assert f"local={out_path}" in out
    assert "[download] Complete" in out
    assert startup_activity_messages == [line for line in out.splitlines() if line]


def test_download_remote_data_file_does_not_log_progress_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    payload = b"1234567890"
    url = "https://example.com/data.parquet"
    startup_activity_messages: list[str] = []

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload, read_chunk_size=5, content_length=len(payload))

    monkeypatch.setattr(data_paths_module, "urlopen", fake_urlopen)
    out_path = data_paths_module.download_remote_data_file(url, tmp_path, activity_messages=startup_activity_messages)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert all(not line.startswith("[download] progress=") for line in startup_activity_messages)
    assert "[download] progress=" not in out


def test_download_remote_data_file_uses_cached_file_when_overwrite_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    url = "https://example.com/data.parquet"
    existing = tmp_path / data_paths_module.remote_filename(url)
    existing_contents = b"PAR1"
    existing.write_bytes(existing_contents)

    def fail_urlopen(_: Any) -> _FakeHttpResponse:
        raise AssertionError("urlopen should not be called when a cached file exists and overwrite is disabled")

    monkeypatch.setattr(data_paths_module, "urlopen", fail_urlopen)
    out_path = data_paths_module.download_remote_data_file(url, tmp_path)

    assert out_path == existing.resolve()
    assert existing.read_bytes() == existing_contents
    out = capsys.readouterr().out
    assert "skipping download" in out
    assert "--overwrite" in out
    assert f"{existing.name}" in out


def test_download_remote_data_file_overwrites_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    url = "https://example.com/data.parquet"
    existing = tmp_path / data_paths_module.remote_filename(url)
    existing.write_bytes(b"old")
    payload = b"new-data"

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(data_paths_module, "urlopen", fake_urlopen)
    out_path = data_paths_module.download_remote_data_file(url, tmp_path, overwrite=True)

    assert out_path == existing.resolve()
    assert existing.read_bytes() == payload
    out = capsys.readouterr().out
    assert "[download] Complete" in out


def test_download_remote_data_file_rejects_file_path_as_download_dir(tmp_path: Path) -> None:
    occupied_path = tmp_path / "occupied"
    occupied_path.write_text("not-a-directory", encoding="utf-8")
    with pytest.raises(typer.BadParameter, match=r"Download path is not a directory"):
        data_paths_module.download_remote_data_file("https://example.com/data.parquet", occupied_path)


def test_download_remote_data_file_cleans_up_partial_file_when_stream_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.com/data.parquet"
    expected = (tmp_path / data_paths_module.remote_filename(url)).resolve()

    def fake_urlopen(request: Any) -> _ExplodingHttpResponse:
        assert request.full_url == url
        return _ExplodingHttpResponse([b"PAR1", b"ABCD"])

    monkeypatch.setattr(data_paths_module, "urlopen", fake_urlopen)

    with pytest.raises(typer.BadParameter, match=r"Failed to download data file"):
        data_paths_module.download_remote_data_file(url, tmp_path)
    assert expected.exists() is False


def test_download_remote_data_file_wraps_error_before_progress_bar_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.com/data.parquet"
    expected = (tmp_path / data_paths_module.remote_filename(url)).resolve()

    def fail_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        raise RuntimeError("network down")

    monkeypatch.setattr(data_paths_module, "urlopen", fail_urlopen)

    with pytest.raises(typer.BadParameter, match=r"Failed to download data file"):
        data_paths_module.download_remote_data_file(url, tmp_path)
    assert expected.exists() is False


@pytest.mark.parametrize(
    "url,expected_suffix",
    [
        ("https://example.com/data.csv", ".csv"),
        ("https://example.com/data.tsv", ".tsv"),
        ("https://example.com/data.txt", ".txt"),
    ],
)
def test_remote_filename_accepts_csv_tsv_and_txt(url: str, expected_suffix: str) -> None:
    filename = data_paths_module.remote_filename(url)
    assert re.fullmatch(rf"data-[0-9a-f]{{12}}{re.escape(expected_suffix)}", filename)


def test_resolve_data_path_rejects_remote_unsupported_extension() -> None:
    with pytest.raises(typer.BadParameter, match=r"Remote URL must end with \.csv, \.tsv, \.txt, \.parquet, or \.pq\."):
        data_paths_module.resolve_data_path("https://example.com/data.json", download_dir=Path("/tmp"))


def test_resolve_data_path_rejects_empty_value(tmp_path: Path) -> None:
    with pytest.raises(typer.BadParameter, match=r"Data path cannot be empty"):
        data_paths_module.resolve_data_path("   ", download_dir=tmp_path)


def test_resolve_data_path_rejects_missing_local_file(tmp_path: Path) -> None:
    with pytest.raises(typer.BadParameter, match=r"Data file not found"):
        data_paths_module.resolve_data_path(str(tmp_path / "missing.csv"), download_dir=tmp_path)


def test_resolve_data_path_rejects_directory_path(tmp_path: Path) -> None:
    folder = tmp_path / "folder"
    folder.mkdir()
    with pytest.raises(typer.BadParameter, match=r"Data path is not a file"):
        data_paths_module.resolve_data_path(str(folder), download_dir=tmp_path)


def test_resolve_data_path_rejects_unreadable_local_file(tmp_path: Path) -> None:
    data_path = (tmp_path / "data.csv").resolve()
    data_path.write_text("x\n1\n", encoding="utf-8")
    data_path.chmod(0o000)
    try:
        with pytest.raises(typer.BadParameter, match=r"Data file is not readable"):
            data_paths_module.resolve_data_path(str(data_path), download_dir=tmp_path)
    finally:
        data_path.chmod(0o600)


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

        def run_input(self, sql_text: str) -> app_module.EngineResponse:
            captured["run_input"] = sql_text
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module.data_paths, "download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, ["--data", remote_url, "--no-ui"])

    assert result.exit_code == 0
    assert captured["download_url"] == remote_url
    assert captured["download_dir"] == _expected_default_download_dir()
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

    monkeypatch.setattr(app_module.data_paths, "download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, ["--data", remote_url, "--overwrite", "--no-ui"])

    assert result.exit_code == 0
    assert captured["download_url"] == remote_url
    assert captured["download_dir"] == _expected_default_download_dir()
    assert captured["overwrite"] is True
    assert isinstance(captured["activity_messages"], list)
    assert captured["data_path"] == downloaded
    assert captured["closed"] is True


def test_main_passes_custom_download_dir_for_remote_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    remote_url = "https://example.com/data.parquet"
    custom_download_dir = tmp_path / "downloads"
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

    monkeypatch.setattr(app_module.data_paths, "download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(
        app_module.app,
        ["--data", remote_url, "--download-dir", str(custom_download_dir), "--no-ui"],
    )

    assert result.exit_code == 0
    assert captured["download_url"] == remote_url
    assert captured["download_dir"] == custom_download_dir.resolve()
    assert captured["overwrite"] is False
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
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            captured["engine"] = engine
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["run"] = True

    monkeypatch.setattr(app_module.data_paths, "download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, ["--data", remote_url])

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

    monkeypatch.setattr(app_module.data_paths, "download_remote_data_file", fail_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, ["--data", str(local_path), "--no-ui"])

    assert result.exit_code == 0
    assert captured["data_path"] == local_path.resolve()
    assert captured["closed"] is True


def test_main_passes_multiple_data_paths_to_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    first.write_text("x\n1\n", encoding="utf-8")
    second.write_text("x\n2\n", encoding="utf-8")
    captured: dict[str, Any] = {}

    class FakeEngine:
        def __init__(
            self,
            data_path: Path,
            table_name: str,
            database: str,
            default_limit: int,
            max_rows_display: int,
            max_value_chars: int,
            data_paths: tuple[Path, ...] | None = None,
        ) -> None:
            _ = table_name
            _ = database
            _ = default_limit
            _ = max_rows_display
            captured["data_path"] = data_path
            captured["data_paths"] = data_paths
            self.default_query = 'SELECT * FROM "data" LIMIT 100'
            self.max_value_chars = max_value_chars

        def run_sql(self, sql_text: str, remember: bool = True) -> app_module.EngineResponse:
            captured["run_sql"] = (sql_text, remember)
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))
    result = runner.invoke(
        app_module.app,
        ["--data", str(first), "--data", str(second), "--no-ui"],
    )

    assert result.exit_code == 0
    assert captured["data_path"] == first.resolve()
    assert captured["data_paths"] == (first.resolve(), second.resolve())
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert captured["closed"] is True


def test_main_rejects_stdin_marker_mixed_with_other_data_values(tmp_path: Path) -> None:
    runner = CliRunner()
    data_path = tmp_path / "a.csv"
    data_path.write_text("x\n1\n", encoding="utf-8")

    result = runner.invoke(app_module.app, ["--data", "-", "--data", str(data_path)], input="alpha\n")
    assert result.exit_code == 2
    assert "When using stdin, provide only --data -." in _strip_ansi(result.output)


def test_main_exits_on_schema_mismatch_for_multiple_data_sources(tmp_path: Path) -> None:
    runner = CliRunner()
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    first.write_text("city,x\na,1\n", encoding="utf-8")
    second.write_text("city,x\nb,nope\n", encoding="utf-8")

    result = runner.invoke(app_module.app, ["--data", str(first), "--data", str(second), "--no-ui"])
    assert result.exit_code == 2
    output = _strip_ansi(result.output)
    assert "Schema mismatch in source 2" in output
    assert second.name in output


def test_main_exits_cleanly_for_corrupt_or_misnamed_parquet_file(tmp_path: Path) -> None:
    runner = CliRunner()
    bad_parquet = tmp_path / "broken.parquet"
    bad_parquet.write_text("hello", encoding="utf-8")

    result = runner.invoke(app_module.app, ["--data", str(bad_parquet), "--no-ui"])

    assert result.exit_code == 2
    output = _strip_ansi(result.output)
    assert "Failed to load source 1" in output
    assert bad_parquet.name in output
    assert "Hint: file extension says Parquet" in output
    assert "Traceback" not in output


def _patch_stdin_fake_engine(
    monkeypatch: pytest.MonkeyPatch,
    captured: dict[str, Any],
    *,
    capture_text: bool = False,
) -> None:
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
            if capture_text:
                captured["data_text"] = data_path.read_text(encoding="utf-8")
            self.default_query = 'SELECT * FROM "data" LIMIT 100'
            self.max_value_chars = max_value_chars

        def run_sql(self, sql_text: str, remember: bool = True) -> app_module.EngineResponse:
            captured["run_sql"] = (sql_text, remember)
            return app_module.EngineResponse(status="ok", message="ok")

        def run_input(self, sql_text: str) -> app_module.EngineResponse:
            captured["run_input"] = sql_text
            return app_module.EngineResponse(status="ok", message="ok")

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))


def _patch_stdin_tty(monkeypatch: pytest.MonkeyPatch, can_run_tui: bool) -> None:
    @contextmanager
    def fake_stdin_tty_for_tui(enabled: bool):
        assert enabled is True
        yield can_run_tui

    monkeypatch.setattr(app_module.stdin_io, "stdin_tty_for_tui", fake_stdin_tty_for_tui)


def test_main_reads_stdin_when_data_arg_is_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["tui_run"] = True

    _patch_stdin_fake_engine(monkeypatch, captured, capture_text=True)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=True)

    result = runner.invoke(app_module.app, ["--data", "-"], input="alpha\nbeta\n")

    assert result.exit_code == 0
    data_path = cast(Path, captured["data_path"])
    assert data_path.suffix == ".txt"
    assert captured["data_text"] == "alpha\nbeta\n"
    assert captured["tui_run"] is True
    startup_messages = cast(list[str], captured["startup_activity_messages"])
    assert any(message.startswith(app_module.stdin_io.STDIN_LOCAL_PREFIX) for message in startup_messages)
    assert captured["closed"] is True
    assert data_path.exists() is False


def test_main_reads_stdin_when_data_arg_is_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["tui_run"] = True

    _patch_stdin_fake_engine(monkeypatch, captured, capture_text=True)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=True)

    result = runner.invoke(app_module.app, [], input="one\ntwo\n")

    assert result.exit_code == 0
    data_path = cast(Path, captured["data_path"])
    assert data_path.suffix == ".txt"
    assert captured["data_text"] == "one\ntwo\n"
    assert captured["tui_run"] is True
    startup_messages = cast(list[str], captured["startup_activity_messages"])
    assert any(message.startswith(app_module.stdin_io.STDIN_LOCAL_PREFIX) for message in startup_messages)
    assert captured["closed"] is True
    assert data_path.exists() is False


def test_main_runs_no_ui_for_stdin_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            raise AssertionError("TUI should not run when --no-ui is set")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, ["--data", "-", "--no-ui"], input="a\nb\n")

    assert result.exit_code == 0
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert captured["closed"] is True


def test_main_uses_execute_as_startup_query_in_tui(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}
    sql = "SELECT line FROM data WHERE line ILIKE '%python%'"

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            startup_query: str | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            captured["startup_activity_messages"] = list(startup_activity_messages or [])
            captured["startup_query"] = startup_query

        def run(self) -> None:
            captured["tui_run"] = True

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=True)

    result = runner.invoke(app_module.app, ["--data", "-", "--execute", sql], input="alpha\npython\n")

    assert result.exit_code == 0
    assert captured["tui_run"] is True
    assert captured["startup_query"] == sql
    assert "run_input" not in captured
    assert "run_sql" not in captured
    assert captured["closed"] is True


def test_main_runs_execute_in_no_ui_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}
    sql = "SELECT line FROM data WHERE line ILIKE '%python%'"

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            raise AssertionError("TUI should not run when --no-ui is set")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, ["--data", "-", "--execute", sql, "--no-ui"], input="alpha\npython\n")

    assert result.exit_code == 0
    assert captured["run_input"] == sql
    assert "run_sql" not in captured
    assert captured["closed"] is True


def test_main_runs_execute_when_tty_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}
    sql = "SELECT line FROM data WHERE line ILIKE '%python%'"

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            raise AssertionError("TUI should not run when tty is unavailable")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=False)

    result = runner.invoke(app_module.app, ["--data", "-", "--execute", sql], input="alpha\npython\n")

    assert result.exit_code == 0
    assert captured["run_input"] == sql
    assert app_module.stdin_io.STDIN_TTY_FALLBACK_MESSAGE in result.output
    assert captured["closed"] is True


def test_main_strips_ansi_escape_sequences_from_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    _patch_stdin_fake_engine(monkeypatch, captured, capture_text=True)
    result = runner.invoke(app_module.app, ["--data", "-", "--no-ui"], input="\x1b[31malpha\x1b[0m\n")

    assert result.exit_code == 0
    assert captured["data_text"] == "alpha\n"
    assert captured["closed"] is True


def test_main_falls_back_to_no_ui_when_tty_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    class FakeTui:
        def __init__(
            self,
            engine: Any,
            startup_activity_messages: list[str] | None = None,
            log_file_path: str | None = None,
        ) -> None:
            _ = log_file_path
            raise AssertionError("TUI should not run when tty is unavailable")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=False)

    result = runner.invoke(app_module.app, ["--data", "-"], input="alpha\n")

    assert result.exit_code == 0
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert app_module.stdin_io.STDIN_TTY_FALLBACK_MESSAGE in result.output
    assert captured["closed"] is True


def test_main_rejects_missing_data_when_stdin_is_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    class _TtyStdin:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(app_module.stdin_io.sys, "stdin", cast(Any, _TtyStdin()))
    with pytest.raises(typer.BadParameter, match=app_module.stdin_io.STDIN_MISSING_SOURCE_ERROR):
        app_module.main(
            data=None,
            table_name="data",
            limit=100,
            max_rows=10000,
            max_value_chars=160,
            database=":memory:",
            execute=None,
            query_file=None,
            no_ui=False,
            overwrite=False,
            download_dir=_expected_default_download_dir(),
            version=False,
        )


def test_main_rejects_empty_stdin_input() -> None:
    runner = CliRunner()
    result = runner.invoke(app_module.app, ["--data", "-"], input="")
    assert result.exit_code == 2
    assert app_module.stdin_io.STDIN_EMPTY_ERROR in result.output


def test_main_rejects_execute_and_query_file_together(tmp_path: Path) -> None:
    runner = CliRunner()
    query_file = tmp_path / "query.sql"
    query_file.write_text("SELECT 1", encoding="utf-8")

    result = runner.invoke(app_module.app, ["--execute", "SELECT 2", "--file", str(query_file)])
    assert result.exit_code == 2
    assert "Use either --execute or --file, not both." in _strip_ansi(result.output)


def test_main_reads_query_file_for_no_ui_execution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}
    query_file = tmp_path / "query.sql"
    query_file.write_text("  SELECT line FROM data  \n", encoding="utf-8")

    _patch_stdin_fake_engine(monkeypatch, captured)
    result = runner.invoke(app_module.app, ["--data", "-", "--file", str(query_file), "--no-ui"], input="alpha\n")

    assert result.exit_code == 0
    assert captured["run_input"] == "SELECT line FROM data"
    assert captured["closed"] is True


def test_render_console_response_renders_generated_sql_and_table_and_error_status() -> None:
    output = StringIO()
    console = app_module.Console(file=output, width=120, color_system=None, force_terminal=False)

    response = app_module.EngineResponse(
        status="error",
        message="query failed",
        generated_sql="SELECT * FROM data",
        result=app_module.QueryResult(
            sql="SELECT * FROM data",
            columns=["name", "score"],
            column_types=["VARCHAR", "INTEGER"],
            rows=[("alphabet", 42)],
            elapsed_ms=12.3,
            total_rows=2,
            truncated=True,
        ),
    )
    exit_code = getattr(app_module, "_render_console_response")(console, response, max_value_chars=5)

    assert exit_code == 1
    text = output.getvalue()
    assert "Generated SQL" in text
    assert "Result (1/2 rows," in text
    assert "12.3 ms)" in text
    assert "al..." in text
    assert "query failed" in text


def test_render_console_response_returns_zero_without_message_panel() -> None:
    output = StringIO()
    console = app_module.Console(file=output, width=100, color_system=None, force_terminal=False)

    response = app_module.EngineResponse(status="ok", message="")
    assert getattr(app_module, "_render_console_response")(console, response, max_value_chars=10) == 0
    assert output.getvalue() == ""


def test_main_version_option_prints_version_without_data(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(app_module, "app_version", lambda: "9.9.9")
    result = runner.invoke(app_module.app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == "sqlexplore 9.9.9"


def test_app_version_falls_back_to_pyproject_when_metadata_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "sqlexplore"\nversion = "1.2.3"\n', encoding="utf-8")
    fake_app_file = tmp_path / "src" / "sqlexplore" / "app.py"
    fake_app_file.parent.mkdir(parents=True)
    fake_app_file.write_text("", encoding="utf-8")

    def fake_importlib_version(_: str) -> str:
        raise engine_module.PackageNotFoundError

    monkeypatch.setattr(engine_module, "__file__", str(fake_app_file))
    monkeypatch.setattr(engine_module, "importlib_version", fake_importlib_version)

    engine_module.app_version.cache_clear()
    try:
        assert app_module.app_version() == "1.2.3"
    finally:
        engine_module.app_version.cache_clear()
