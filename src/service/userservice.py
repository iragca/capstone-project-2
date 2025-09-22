from asyncio import run
from typing import Optional

from src.db import PBWarehouse
from src.models import User
from src.scraper import TweetyScraper


class UserService:
    """
    Service for retrieving user information from the database or,
    if missing, scraping from the web and persisting the result.

    Parameters
    ----------
    pb : PBWarehouse
        The persistent storage backend for users and tweets.
    scraper : TweetyScraper
        Scraper instance for fetching user information.
    """

    def __init__(self, pb: PBWarehouse, scraper: TweetyScraper):
        self.pb = pb
        self.scraper = scraper

    def get_or_fetch_user(
        self,
        username: str,
        user_id: Optional[str] = None,
        strict_matching: bool = False,
    ) -> Optional[User]:
        """
        Retrieve a user from the warehouse or fetch with the scraper if not found.

        Parameters
        ----------
        username : str
            The username of the user to retrieve.
        user_id : str, optional
            The user ID if available. If not provided, lookup is done by username.
        strict_matching : bool, default=False
            Whether to enforce strict username matching in the warehouse.

        Returns
        -------
        User or None
            The User object, or None if not found and could not be scraped.
        """
        user_exists = self.pb.does_user_exist(
            username=username, user_id=user_id, strict=strict_matching
        )

        if user_exists:
            if user_id is not None:
                user_record = self.pb.get_user_by_id(user_id)
            else:
                user_record = self.pb.get_user_by_username(
                    username, strict=strict_matching
                )
            return User(**user_record.__dict__)

        # Fallback: scrape the user
        user = run(
            self.scraper.get_user_info(int(user_id) if user_id else None, username)
        )

        if user is None:
            return None

        # Persist scraped user
        self.pb.ingest_user(user)
        return user
