import asyncio
import json
from pipeline.sources import ReplaySource

def test_replay_source_reads_all_lines(tmp_path):
    """ReplaySource đọc đầy đủ các dòng JSON từ file và chuyển thành dict."""
    sample_file = tmp_path / "sample.jsonl"
    lines = [
        {"e": "trade", "t": 1, "p": "80000.0"},
        {"e": "trade", "t": 2, "p": "80010.0"},
        {"e": "trade", "t": 3, "p": "80020.0"},
    ]
    sample_file.write_text("\n".join(json.dumps(x) for x in lines) + "\n", encoding="utf-8")

    async def _run():
        source = ReplaySource(sample_file, speed=0.0)
        return [item async for item in source.stream()]

    collected = asyncio.run(_run())

    assert len(collected) == 3
    assert collected[0]["t"] == 1
    assert collected[1]["p"] == "80010.0"
    assert collected[2]["t"] == 3

def test_replay_source_ignores_empty_lines(tmp_path):
    """ReplaySource bỏ qua các dòng rỗng hoặc khoảng trắng thừa."""
    sample_file = tmp_path / "sample_with_blanks.jsonl"
    content = '{"e": "trade", "t": 1}\n\n   \n{"e": "trade", "t": 2}\n\n'
    sample_file.write_text(content, encoding="utf-8")

    async def _run():
        source = ReplaySource(sample_file, speed=0.0)
        return [item async for item in source.stream()]

    collected = asyncio.run(_run())

    assert len(collected) == 2
    assert collected[0]["t"] == 1
    assert collected[1]["t"] == 2
