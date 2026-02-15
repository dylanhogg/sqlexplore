from sqlexplore.ui.image_cells import (
    MAX_IMAGE_BYTES,
    format_image_cell_token,
    format_image_preview_metadata,
    summarize_image_cell,
)

PNG_1X1_HEX = (
    "89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C489"
    "0000000A49444154789C6360000000020001E221BC330000000049454E44AE426082"
)


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
