"""Unit tests for demodulation parsing and decoding logic."""

from __future__ import annotations

from datetime import UTC, datetime

from cts1_data_pipeline.decoding.decoder import _decode_frame
from cts1_data_pipeline.demodulation.runner import _parse_hexdump_line


class TestParseHexdumpLine:
    def test_valid_line_with_subseconds(self) -> None:
        line = "2024-03-15 12:34:56.789 [hexdump] AA BB CC DD"
        result = _parse_hexdump_line(line)
        assert result is not None
        ts, hex_data = result
        assert ts == datetime(2024, 3, 15, 12, 34, 56, 789000, tzinfo=UTC)
        assert hex_data == "AA BB CC DD"

    def test_valid_line_without_subseconds(self) -> None:
        line = "2024-03-15 12:34:56 [hexdump] DE AD BE EF"
        result = _parse_hexdump_line(line)
        assert result is not None
        ts, hex_data = result
        assert ts == datetime(2024, 3, 15, 12, 34, 56, tzinfo=UTC)
        assert hex_data == "DE AD BE EF"

    def test_non_hexdump_line(self) -> None:
        line = "2024-03-15 12:34:56 [info] Some info message"
        assert _parse_hexdump_line(line) is None

    def test_empty_line(self) -> None:
        assert _parse_hexdump_line("") is None

    def test_garbage_line(self) -> None:
        assert _parse_hexdump_line("not a real log line at all") is None


class TestDecodeFrame:
    def test_decode_with_enough_bytes(self) -> None:
        # 10 bytes: covers all field specs
        payload = bytes(range(10))
        hex_str = payload.hex()
        fields = _decode_frame(hex_str)
        assert "raw_hex" in fields
        assert "eps_battery_voltage_mv" in fields
        assert "obc_uptime_s" in fields

    def test_decode_short_payload(self) -> None:
        # Only 2 bytes — only the first field should be present
        payload = bytes([0x0F, 0xA0])
        fields = _decode_frame(payload.hex())
        assert "eps_battery_voltage_mv" in fields
        assert "obc_uptime_s" not in fields

    def test_decode_invalid_hex(self) -> None:
        fields = _decode_frame("ZZ ZZ not hex")
        assert fields == {}

    def test_raw_hex_always_uppercase(self) -> None:
        fields = _decode_frame("deadbeef")
        assert fields["raw_hex"] == "DEADBEEF"
