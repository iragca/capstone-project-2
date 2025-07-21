from ...models import User


def api45_user(user_data: dict) -> User:
    """
    Parses a user data dictionary into a User model instance.

    Args:
        user_data (dict): The user data dictionary.

    Returns:
        User: An instance of the User model.
    """
    assert isinstance(user_data, dict), "Input must be a dictionary"

    try:
        user_id = user_data.get("rest_id")
        username = user_data.get("screen_name")
        name = user_data.get("name", "")
        description = user_data.get("description", "")
        followers_count = user_data.get("followers_count", 0)
        friends_count = user_data.get("friends_count", 0)
        is_blue_verified = user_data.get("verified", False)
        created_at = user_data.get("created_at", "")
        location = user_data.get("location", None)
    except Exception as e:
        raise ValueError(f"Error processing user data: {e}, {user_data}")

    return User(
        user_id=user_id,
        username=username,
        name=name,
        description=description,
        follower_count=followers_count,
        friends=friends_count,
        is_blue_verified=is_blue_verified,
        creation_date=created_at,
        location=location,
        is_private=None,
        is_verified=None,
        bot=None,
    )