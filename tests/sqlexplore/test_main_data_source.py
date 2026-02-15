from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

import sqlexplore.app as app_module
import sqlexplore.core.engine as engine_module


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


def test_download_remote_data_file_writes_file_and_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    payload = b"PAR1....test-bytes...."
    url = "https://example.com/data.parquet"
    startup_activity_messages: list[str] = []

    def fake_urlopen(request: Any) -> _FakeHttpResponse:
        assert request.full_url == url
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)

    download_remote = getattr(app_module, "_download_remote_data_file")
    out_path = download_remote(url, tmp_path, activity_messages=startup_activity_messages)

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

    monkeypatch.setattr(app_module, "urlopen", fake_urlopen)

    download_remote = getattr(app_module, "_download_remote_data_file")
    out_path = download_remote(url, tmp_path, activity_messages=startup_activity_messages)

    assert out_path.read_bytes() == payload
    out = capsys.readouterr().out
    assert all(not line.startswith("[download] progress=") for line in startup_activity_messages)
    assert "[download] progress=" not in out


def test_download_remote_data_file_uses_cached_file_when_overwrite_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    existing = tmp_path / "data.parquet"
    existing_contents = b"PAR1"
    existing.write_bytes(existing_contents)

    def fail_urlopen(_: Any) -> _FakeHttpResponse:
        raise AssertionError("urlopen should not be called when a cached file exists and overwrite is disabled")

    monkeypatch.setattr(app_module, "urlopen", fail_urlopen)

    download_remote = getattr(app_module, "_download_remote_data_file")
    out_path = download_remote("https://example.com/data.parquet", tmp_path)

    assert out_path == existing.resolve()
    assert existing.read_bytes() == existing_contents
    out = capsys.readouterr().out
    assert "skipping download" in out
    assert "--overwrite" in out
    assert f"{existing.name}" in out


def test_download_remote_data_file_overwrites_when_enabled(
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
    download_remote = getattr(app_module, "_download_remote_data_file")
    out_path = download_remote(url, tmp_path, overwrite=True)

    assert out_path == existing.resolve()
    assert existing.read_bytes() == payload
    out = capsys.readouterr().out
    assert "[download] Complete" in out


def test_download_remote_data_file_rejects_file_path_as_download_dir(tmp_path: Path) -> None:
    occupied_path = tmp_path / "occupied"
    occupied_path.write_text("not-a-directory", encoding="utf-8")
    download_remote = getattr(app_module, "_download_remote_data_file")
    with pytest.raises(typer.BadParameter, match=r"Download path is not a directory"):
        download_remote("https://example.com/data.parquet", occupied_path)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://example.com/data.csv", "data.csv"),
        ("https://example.com/data.tsv", "data.tsv"),
        ("https://example.com/data.txt", "data.txt"),
    ],
)
def test_remote_filename_accepts_csv_tsv_and_txt(url: str, expected: str) -> None:
    remote_filename = getattr(app_module, "_remote_filename")
    assert remote_filename(url) == expected


def test_resolve_data_path_rejects_remote_unsupported_extension() -> None:
    resolve_data_path = getattr(app_module, "_resolve_data_path")
    with pytest.raises(typer.BadParameter, match=r"Remote URL must end with \.csv, \.tsv, \.txt, \.parquet, or \.pq\."):
        resolve_data_path("https://example.com/data.json", download_dir=Path("/tmp"))


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

    monkeypatch.setattr(app_module, "_download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [remote_url, "--no-ui"])

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

    monkeypatch.setattr(app_module, "_download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [remote_url, "--overwrite", "--no-ui"])

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

    monkeypatch.setattr(app_module, "_download_remote_data_file", fake_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(
        app_module.app,
        [remote_url, "--download-dir", str(custom_download_dir), "--no-ui"],
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
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            captured["engine"] = engine
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["run"] = True

    monkeypatch.setattr(app_module, "_download_remote_data_file", fake_download)
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

    monkeypatch.setattr(app_module, "_download_remote_data_file", fail_download)
    monkeypatch.setattr(app_module, "SqlExplorerEngine", cast(Any, FakeEngine))

    result = runner.invoke(app_module.app, [str(local_path), "--no-ui"])

    assert result.exit_code == 0
    assert captured["data_path"] == local_path.resolve()
    assert captured["closed"] is True


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
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            captured["startup_activity_messages"] = list(startup_activity_messages or [])

        def run(self) -> None:
            captured["tui_run"] = True

    _patch_stdin_fake_engine(monkeypatch, captured, capture_text=True)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=True)

    result = runner.invoke(app_module.app, ["-"], input="alpha\nbeta\n")

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
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
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
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            raise AssertionError("TUI should not run when --no-ui is set")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, ["-", "--no-ui"], input="a\nb\n")

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
        ) -> None:
            captured["startup_activity_messages"] = list(startup_activity_messages or [])
            captured["startup_query"] = startup_query

        def run(self) -> None:
            captured["tui_run"] = True

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=True)

    result = runner.invoke(app_module.app, ["-", "--execute", sql], input="alpha\npython\n")

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
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            raise AssertionError("TUI should not run when --no-ui is set")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))

    result = runner.invoke(app_module.app, ["-", "--execute", sql, "--no-ui"], input="alpha\npython\n")

    assert result.exit_code == 0
    assert captured["run_input"] == sql
    assert "run_sql" not in captured
    assert captured["closed"] is True


def test_main_runs_execute_when_tty_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}
    sql = "SELECT line FROM data WHERE line ILIKE '%python%'"

    class FakeTui:
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            raise AssertionError("TUI should not run when tty is unavailable")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=False)

    result = runner.invoke(app_module.app, ["-", "--execute", sql], input="alpha\npython\n")

    assert result.exit_code == 0
    assert captured["run_input"] == sql
    assert app_module.stdin_io.STDIN_TTY_FALLBACK_MESSAGE in result.output
    assert captured["closed"] is True


def test_main_strips_ansi_escape_sequences_from_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    _patch_stdin_fake_engine(monkeypatch, captured, capture_text=True)
    result = runner.invoke(app_module.app, ["-", "--no-ui"], input="\x1b[31malpha\x1b[0m\n")

    assert result.exit_code == 0
    assert captured["data_text"] == "alpha\n"
    assert captured["closed"] is True


def test_main_falls_back_to_no_ui_when_tty_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    captured: dict[str, Any] = {}

    class FakeTui:
        def __init__(self, engine: Any, startup_activity_messages: list[str] | None = None) -> None:
            raise AssertionError("TUI should not run when tty is unavailable")

    _patch_stdin_fake_engine(monkeypatch, captured)
    monkeypatch.setattr(app_module, "SqlExplorerTui", cast(Any, FakeTui))
    _patch_stdin_tty(monkeypatch, can_run_tui=False)

    result = runner.invoke(app_module.app, ["-"], input="alpha\n")

    assert result.exit_code == 0
    assert captured["run_sql"] == ('SELECT * FROM "data" LIMIT 100', False)
    assert app_module.stdin_io.STDIN_TTY_FALLBACK_MESSAGE in result.output
    assert captured["closed"] is True


def test_main_rejects_missing_data_when_stdin_is_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    class _TtyStdin:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(app_module.sys, "stdin", cast(Any, _TtyStdin()))
    with pytest.raises(typer.BadParameter, match=app_module.stdin_io.STDIN_MISSING_SOURCE_ERROR):
        app_module.main(
            data=None,
            table_name="data",
            limit=100,
            max_rows=1000,
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
    result = runner.invoke(app_module.app, ["-"], input="")
    assert result.exit_code == 2
    assert app_module.stdin_io.STDIN_EMPTY_ERROR in result.output


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
