import pytest

from sqlexplore.ui.image_cells import (
    MAX_IMAGE_BYTES,
    ImageCellInfo,
    format_image_cell_token,
    format_image_preview_metadata,
    summarize_image_cell,
)

PNG_1X1_HEX = (
    "89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C489"
    "0000000A49444154789C6360000000020001E221BC330000000049454E44AE426082"
)
PNG_BAD_DIMENSIONS_HEX = (
    "89504E470D0A1A0A0000000D000000000000000108060000001F15C489"
    "0000000A49444154789C6360000000020001E221BC330000000049454E44AE426082"
)
GIF_1X2 = b"GIF89a\x01\x00\x02\x00\x00\x00\x00"
GIF_ZERO_WIDTH = b"GIF89a\x00\x00\x02\x00\x00\x00\x00"
BMP_SAMPLE = b"BM" + (b"\x00" * 14)
WEBP_SAMPLE = b"RIFF\x10\x00\x00\x00WEBP" + (b"\x00" * 4)
TIFF_SAMPLE = b"II*\x00" + (b"\x00" * 8)
JPEG_WITH_SOF0_3X2 = bytes.fromhex("FFD8FFE000104A46494600010100000100010000FFC00011080002000303011100021100031100FFD9")
JPEG_BAD_SEGMENT = bytes.fromhex("FFD8FFE00001FFD9")


def test_summarize_image_cell_detects_struct_bytes_png() -> None:
    payload = bytes.fromhex(PNG_1X1_HEX)
    value = {"bytes": payload, "path": " /tmp/example.png "}
    image = summarize_image_cell(value)

    assert image is not None
    assert image.format_name == "png"
    assert image.width == 1
    assert image.height == 1
    assert image.source == "struct.bytes"
    assert image.path == "/tmp/example.png"
    assert format_image_cell_token(image) == "[img png 1x1 67 B]"
    preview = format_image_preview_metadata(image)
    assert "format: png" in preview
    assert "dimensions: 1x1" in preview


def test_summarize_image_cell_detects_raw_bytes_jpeg() -> None:
    jpeg = bytes.fromhex("FFD8FFE000104A4649460001FFD9")
    image = summarize_image_cell(jpeg)

    assert image is not None
    assert image.format_name == "jpeg"
    assert image.size_bytes == len(jpeg)
    assert image.source == "bytes"
    assert image.width is None
    assert image.height is None


def test_summarize_image_cell_extracts_jpeg_dimensions_from_sof_marker() -> None:
    image = summarize_image_cell(JPEG_WITH_SOF0_3X2)

    assert image is not None
    assert image.format_name == "jpeg"
    assert image.width == 3
    assert image.height == 2


def test_summarize_image_cell_extracts_gif_dimensions() -> None:
    image = summarize_image_cell(GIF_1X2)

    assert image is not None
    assert image.format_name == "gif"
    assert image.width == 1
    assert image.height == 2


@pytest.mark.parametrize(
    ("payload", "format_name"),
    [
        (BMP_SAMPLE, "bmp"),
        (WEBP_SAMPLE, "webp"),
        (TIFF_SAMPLE, "tiff"),
    ],
)
def test_summarize_image_cell_detects_additional_binary_formats(payload: bytes, format_name: str) -> None:
    image = summarize_image_cell(payload)

    assert image is not None
    assert image.format_name == format_name
    assert image.width is None
    assert image.height is None


def test_summarize_image_cell_returns_none_for_empty_payload() -> None:
    assert summarize_image_cell(b"") is None
    assert summarize_image_cell({"bytes": b"", "path": "/tmp/empty.bin"}) is None


def test_summarize_image_cell_accepts_memoryview_payload() -> None:
    jpeg = bytes.fromhex("FFD8FFE000104A4649460001FFD9")
    image = summarize_image_cell({"bytes": memoryview(jpeg), "path": None})

    assert image is not None
    assert image.format_name == "jpeg"
    assert image.source == "struct.bytes"


def test_summarize_image_cell_returns_none_for_non_image_payload() -> None:
    assert summarize_image_cell({"bytes": b"not-image", "path": None}) is None


def test_summarize_image_cell_rejects_string_payloads() -> None:
    assert summarize_image_cell({"bytes": "b'\\xff\\xd8\\xff\\xd9'", "path": None}) is None
    assert summarize_image_cell('{"bytes":"..."}') is None


def test_summarize_image_cell_rejects_oversized_payload() -> None:
    oversized = b"\xff\xd8\xff" + (b"\x00" * MAX_IMAGE_BYTES)
    assert len(oversized) > MAX_IMAGE_BYTES
    assert summarize_image_cell(oversized) is None


def test_summarize_image_cell_handles_invalid_or_missing_dimensions() -> None:
    png_bad_dims = bytes.fromhex(PNG_BAD_DIMENSIONS_HEX)
    png = summarize_image_cell(png_bad_dims)
    assert png is not None
    assert png.format_name == "png"
    assert png.width is None
    assert png.height is None

    gif = summarize_image_cell(GIF_ZERO_WIDTH)
    assert gif is not None
    assert gif.format_name == "gif"
    assert gif.width is None
    assert gif.height is None

    jpeg = summarize_image_cell(JPEG_BAD_SEGMENT)
    assert jpeg is not None
    assert jpeg.format_name == "jpeg"
    assert jpeg.width is None
    assert jpeg.height is None


@pytest.mark.parametrize(
    ("size_bytes", "expected"),
    [
        (1024, "[img png 1.0 KB]"),
        (1024 * 1024, "[img png 1.0 MB]"),
        (1024 * 1024 * 1024, "[img png 1.0 GB]"),
    ],
)
def test_format_image_cell_token_formats_kb_mb_and_gb(size_bytes: int, expected: str) -> None:
    image = ImageCellInfo(
        format_name="png",
        size_bytes=size_bytes,
        width=None,
        height=None,
        sha1_short="deadbeef00",
        source="bytes",
        path=None,
    )
    assert format_image_cell_token(image) == expected
