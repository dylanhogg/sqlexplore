import hashlib
import struct
from dataclasses import dataclass
from typing import Mapping, cast

MAX_IMAGE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ImageCellInfo:
    format_name: str
    size_bytes: int
    width: int | None
    height: int | None
    sha1_short: str
    source: str
    path: str | None


def _format_byte_count(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _coerce_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    path = value.strip()
    return path or None


def _coerce_bytes(value: object) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


def _image_format(data: bytes) -> str | None:
    if len(data) < 4:
        return None
    if data.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    if data.startswith(b"BM"):
        return "bmp"
    if data[:4] == b"RIFF" and len(data) >= 12 and data[8:12] == b"WEBP":
        return "webp"
    if data.startswith(b"II*\x00") or data.startswith(b"MM\x00*"):
        return "tiff"
    return None


def _png_dimensions(data: bytes) -> tuple[int | None, int | None]:
    if len(data) < 24 or data[12:16] != b"IHDR":
        return None, None
    width, height = struct.unpack(">II", data[16:24])
    if width <= 0 or height <= 0:
        return None, None
    return width, height


def _gif_dimensions(data: bytes) -> tuple[int | None, int | None]:
    if len(data) < 10:
        return None, None
    width, height = struct.unpack("<HH", data[6:10])
    if width <= 0 or height <= 0:
        return None, None
    return width, height


def _jpeg_dimensions(data: bytes) -> tuple[int | None, int | None]:
    index = 2
    total = len(data)
    while index + 9 < total:
        if data[index] != 0xFF:
            index += 1
            continue
        while index < total and data[index] == 0xFF:
            index += 1
        if index >= total:
            break
        marker = data[index]
        index += 1
        if marker in {0xD8, 0xD9, 0x01} or 0xD0 <= marker <= 0xD7:
            continue
        if index + 1 >= total:
            break
        segment_length = int.from_bytes(data[index : index + 2], "big")
        if segment_length < 2 or index + segment_length > total:
            break
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            if segment_length < 7:
                break
            height = int.from_bytes(data[index + 3 : index + 5], "big")
            width = int.from_bytes(data[index + 5 : index + 7], "big")
            if width > 0 and height > 0:
                return width, height
            break
        index += segment_length
    return None, None


def _image_dimensions(format_name: str, data: bytes) -> tuple[int | None, int | None]:
    if format_name == "png":
        return _png_dimensions(data)
    if format_name == "gif":
        return _gif_dimensions(data)
    if format_name == "jpeg":
        return _jpeg_dimensions(data)
    return None, None


def _normalize_image_payload(value: object) -> tuple[bytes, str, str | None] | None:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        payload = _coerce_bytes(mapping.get("bytes"))
        if payload is not None:
            return payload, "struct.bytes", _coerce_path(mapping.get("path"))
        return None

    payload = _coerce_bytes(value)
    if payload is None:
        return None
    return payload, "bytes", None


def summarize_image_cell(value: object) -> ImageCellInfo | None:
    normalized = _normalize_image_payload(value)
    if normalized is None:
        return None
    payload, source, path = normalized
    if not payload:
        return None
    if len(payload) > MAX_IMAGE_BYTES:
        return None
    format_name = _image_format(payload)
    if format_name is None:
        return None
    width, height = _image_dimensions(format_name, payload)
    sha1_short = hashlib.sha1(payload).hexdigest()[:10]
    return ImageCellInfo(
        format_name=format_name,
        size_bytes=len(payload),
        width=width,
        height=height,
        sha1_short=sha1_short,
        source=source,
        path=path,
    )


def format_image_cell_token(image: ImageCellInfo) -> str:
    token = f"[img {image.format_name}"
    if image.width is not None and image.height is not None:
        token += f" {image.width}x{image.height}"
    token += f" {_format_byte_count(image.size_bytes)}]"
    return token


def format_image_preview_metadata(image: ImageCellInfo) -> str:
    dimensions = f"{image.width}x{image.height}" if image.width is not None and image.height is not None else "unknown"
    lines = [
        "[image]",
        f"format: {image.format_name}",
        f"dimensions: {dimensions}",
        f"size: {_format_byte_count(image.size_bytes)} ({image.size_bytes:,} bytes)",
        f"sha1: {image.sha1_short}",
        f"source: {image.source}",
    ]
    if image.path is not None:
        lines.append(f"path: {image.path}")
    return "\n".join(lines)
