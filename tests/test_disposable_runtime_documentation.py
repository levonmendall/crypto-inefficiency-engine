from pathlib import Path


def test_memory_runtime_documentation_preserves_paper_only_governance():
    text = Path("MEMORY_RUNTIME_ARCHITECTURE.md").read_text(encoding="utf-8")
    assert "paper-only" in text
    assert "mutually exclusive" in text
    assert "cgroup" in text
    assert "default four assets" in text
