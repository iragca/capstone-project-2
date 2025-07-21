def blm_hashtag(text: str) -> bool:
    """
    Checks if the tweet contains the #BLM hashtag.

    Args:
        tweet (Tweet): The tweet to check.

    Returns:
        bool: True if the tweet contains the #BLM hashtag, False otherwise.
    """
    search_terms = [
        "#blacklivesmatter",
        "#blm",
        "#blacklivesmatters",
    ]

    return any(hashtag in text for hashtag in search_terms)
