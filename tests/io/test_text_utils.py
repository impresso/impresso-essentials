import pytest
from impresso_essentials.text_utils import tokenise


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
        # Sentence with missing language rule, fallback to default splitting
        ("Hello world!", "other", ["Hello", "world!"]),
        # Multiple spaces between words
        ("Hello   world", "en", ["Hello", "world"]),
        # Text with newline and tab characters
        ("Hello\nworld\t!", "en", ["Hello", "world", "!"]),
    ],
)
def test_tokenise(text, language, expected):
    assert tokenise(text, language) == expected
