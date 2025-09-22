# Database

A [PocketBase](https://pocketbase.io) instance will be used for storing the {ref}`tweet` and {ref}`user` data.

The instance we mainly use is self-hosted and can be found on https://capstone.gari-homelab.party/_/#/.

## Tables

We have two main tables for both {ref}`tweet` and {ref}`user` data named `tweets_v2` and `tweet_users` respectively.

We also have view tables that make the data easier to supervise.

- `annotated_tweets`
- `extremist_tweets`
- `has_replies`
- `num_tweets_vs_actual_user_tweets`
- `user_tweets_status`
- `dataset`

## `PBWarehouse`

A service wrapper around a [PocketBase](https://pocketbase.io) instance and the [PocketBase-sdk](https://github.com/vaphes/pocketbase), providing methods for ingesting and retrieving `Tweet` and `User` records. It handles authentication, record creation, updates, and queries while adding validation and logging.

```{eval-rst}
.. autoclass:: src.db.PBWarehouse
   :members:
   :undoc-members:
   :show-inheritance:
```

### Example Usage

```python
warehouse = PBWarehouse()

# Ingest a user
user = User(user_id="123", description="Test user")
record = warehouse.ingest_user(user)

# Ingest a tweet
tweet = Tweet(
    tweet_id="456",
    text="Hello #BLM",
    status_link="https://x.com/test/status/456",
    user_id="123",
    description="tweet description",
    has_blm_hashtag=True,
    language="en",
    creation_date="2024-01-01",
    bookmark_count=0,
    retweet_count=0,
    favorite_count=0,
    reply_count=0,
    quote_count=0,
    conversation_id="456"
)
record = warehouse.ingest_single_tweet(tweet)

# Query dataset
dataset = warehouse.get_dataset()
```
