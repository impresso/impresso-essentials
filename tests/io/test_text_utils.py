import pytest
from impresso_essentials.text_utils import tokenise, WHITESPACE_RULES


@pytest.mark.parametrize(
    "text,language,expected",
    [
        # Empty string
        ("", "en", []),
        # Single word, no punctuation
        ("hello", "en", ["hello"]),
        # Sentence with punctuation (English rules)
        ("Hello, world!", "en", ["Hello", ",", "world", "!"]),
        # Sentence with punctuation (French rules)
        ("Bonjour, le monde!", "fr", ["Bonjour", ",", "le", "monde", "!"]),
        # Sentence with punctuation (French quotes)
        ("«Bonjour le monde»", "fr", ["«", "Bonjour", "le", "monde", "»"]),
        # Sentence with parentheses and brackets
        ("(Hello) [world]!", "en", ["(", "Hello", ")", "[", "world", "]", "!"]),
        # Sentence with missing language rule, fallback to default splitting (other)
        ("Hello world!", "es", ["Hello", "world!"]),
        # Multiple spaces between words
        ("Hello   world", "en", ["Hello", "world"]),
        # Text with newline and tab characters
        ("Hello\nworld\t!", "en", ["Hello", "world", "!"]),
    ],
)
def test_tokenise(text, language, expected):
    assert tokenise(text, language) == expected


def test_whitespace_rules_keys():
    """Ensure that the necessary keys exist in WHITESPACE_RULES."""
    for language, rules in WHITESPACE_RULES.items():
        assert (
            "pct_no_ws_before_after" in rules
        ), f"Missing key 'pct_no_ws_before_after' for {language}"
        assert (
            "pct_no_ws_before" in rules
        ), f"Missing key 'pct_no_ws_before' for {language}"
        assert (
            "pct_no_ws_after" in rules
        ), f"Missing key 'pct_no_ws_after' for {language}"
