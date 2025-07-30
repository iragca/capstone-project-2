from datetime import datetime

import streamlit as st

from src.db import PBWarehouse
from src.models import Tweet, User


def show_tweet(tweet: Tweet, pb: PBWarehouse, probability: float = None):
    user_record = pb.get_user_by_id(tweet.user_id)
    if user_record is None:
        st.error(f"User with ID {tweet.user_id} not found.")
        return
    user = User(**user_record.__dict__)
    name = f"{user.name}"
    if user.is_blue_verified:
        name += " :blue-badge[:material/check: Verified]"

    date = datetime.fromisoformat(tweet.creation_date)

    if probability < 0.6:
        prob_color = "green"
        prob_label = "Low"
    elif probability < 0.8:
        prob_color = "yellow"
        prob_label = "Medium"
    else:
        prob_color = "red"
        prob_label = "High"

    with st.container(key=tweet.tweet_id, border=True):
        st.markdown(

            f"""
    :{prob_color}-badge[{probability:.2f} - {prob_label}] {name}
    <span style="opacity: 0.5; font-size:1rem;">
        @{user.username} · 
        <a href="https://x.com/{user.username}/status/{tweet.tweet_id}" 
           target="_blank" style="color: inherit;">
           {date.strftime("%b %d")}
        </a>
    </span>
    """,
            unsafe_allow_html=True,
        )

        st.markdown(tweet.text)

        st.markdown(
            f":gray-badge[:material/mode_comment: {tweet.reply_count}] "
            f":gray-badge[:material/share: {tweet.retweet_count}] "
            f":gray-badge[:material/favorite: {tweet.favorite_count}]"
        )
