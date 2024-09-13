"""Reusable util functions to perform some text processing.
"""

import re
import logging
import pysbd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

logger = logging.getLogger(__name__)


WHITESPACE_RULES = {
    "fr": {
        "punctuation_nows_before": [".", ",", ")", "]", "}", "°", "..."],
        "punctuation_nows_after": ["(", "[", "{"],
        "punctuation_nows_beforeafter": ["'", "-"],
        "punctuation_ciffre": [".", ","],
    }
}


def segment_and_trim_sentences(article, language, max_length):

    try:
        segmenter = pysbd.Segmenter(language=language, clean=False)
    except Exception:
        segmenter = pysbd.Segmenter(language="en", clean=False)

    sentences = segmenter.segment(article)

    trimmed_sentences = []
    for sentence in sentences:
        while len(sentence) > max_length:
            # Find the last space within max_length
            cut_index = sentence.rfind(" ", 0, max_length)
            if cut_index == -1:
                # If no space found, forcibly cut at max_length
                cut_index = max_length

            # Cut the sentence and add the first part to trimmed sentences
            trimmed_sentences.append(sentence[:cut_index])

            # Update the sentence to be the remaining part
            sentence = sentence[cut_index:].lstrip()

        # Add the remaining part of the sentence if it's not empty
        if sentence:
            trimmed_sentences.append(sentence)

    return trimmed_sentences


def is_stopword_or_all_stopwords(text: str, languages=["french", "german"]) -> bool:
    """Check if the text is a stopword in the specified languages or if all tokens in the text are stopwords.

    Args:
        text (str): The text to check.
        languages (list): List of languages to consider for stopwords.

    Returns:
        bool: True if the text is a stopword or all tokens are stopwords, False otherwise.
    """
    # Load stopwords for the specified languages
    stopwords_list = set()
    for lang in languages:
        stopwords_list.update(stopwords.words(lang))

    # Tokenize the text
    tokens = word_tokenize(text)

    # Check if the text is a single stopword
    if len(tokens) == 1 and tokens[0].lower() in stopwords_list:
        return True

    # Check if all tokens are stopwords
    return all(token.lower() in stopwords_list for token in tokens)


def tokenise(text: str, language: str) -> list[str]:
    """Apply whitespace rules to the provided text for a specific language and return a list of tokens including punctuation as separate tokens.

    Args:
        text (str): The input text.
        language (str): Language of the text.

    Returns:
        list[str]: List of tokens with punctuation as separate tokens.
    """
    # text = add_spaces_around_punctuation(text)
    if not text:
        return []

    if language not in WHITESPACE_RULES:
        # Default behavior for languages without specific rules: tokenize using standard whitespace splitting
        return text.split()

    wsrules = WHITESPACE_RULES[language]
    tokenized_text = []
    current_token = ""
    # print('tokens before=', text.split())
    for char in text:
        if char in wsrules["punctuation_nows_beforeafter"]:
            if current_token:
                tokenized_text.append(current_token)
            tokenized_text.append(char)
            current_token = ""
        elif (
            char in wsrules["punctuation_nows_before"]
            or char in wsrules["punctuation_nows_after"]
        ):
            if current_token:
                tokenized_text.append(current_token)
            tokenized_text.append(char)
            current_token = ""
        elif char.isspace():
            if current_token:
                tokenized_text.append(current_token)
                current_token = ""
        else:
            current_token += char

    if current_token:
        tokenized_text.append(current_token)

    # print('tokens after=', tokenized_text)
    return tokenized_text


def normalize_text(text):
    # Remove spaces and tabs for the search but keep newline characters
    return re.sub(r"[ \t]+", "", text)


def search_text(article_text, search_text):
    # Normalize texts by removing spaces and tabs
    normalized_article = normalize_text(article_text)
    normalized_search = normalize_text(search_text)

    # Initialize a list to hold all start and end indices
    indices = []

    # Find all occurrences of the search text in the normalized article text
    start_index = 0
    while True:
        start_index = normalized_article.find(normalized_search, start_index)
        if start_index == -1:
            break

        # Calculate the actual start and end indices in the original article text
        original_chars = 0
        original_start_index = 0
        for i in range(start_index):
            while article_text[original_start_index] in (" ", "\t"):
                original_start_index += 1
            if article_text[original_start_index] not in (" ", "\t", "\n"):
                original_chars += 1
            original_start_index += 1

        original_end_index = original_start_index
        search_chars = 0
        while search_chars < len(normalized_search):
            if article_text[original_end_index] not in (" ", "\t", "\n"):
                search_chars += 1
            original_end_index += 1  # Increment to include the last character

        if article_text[original_start_index] == " ":
            original_start_index += 1

        # Append the found indices to the list
        indices.append((original_start_index, original_end_index))

        # Move start_index to the next position to continue searching
        start_index += 1

    return indices
