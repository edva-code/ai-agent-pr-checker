import pytest

from tracker.report import END, START, replace_block


def test_replace_block_keeps_surrounding_text():
    text = f"intro\n{START}\nold\n{END}\noutro"
    assert replace_block(text, "new") == f"intro\n{START}\nnew\n{END}\noutro"


def test_replace_block_fails_without_markers():
    with pytest.raises(ValueError):
        replace_block("no markers here", "new")
