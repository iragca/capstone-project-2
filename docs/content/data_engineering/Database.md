# Database

A [PocketBase](https://pocketbase.io) instance will be used for storing the {ref}`tweet` and {ref}`user` data.

The instance we mainly use is self-hosted and can be found on https://capstone.gari-homelab.party/_/#/.

## Tables

We have two main tables for both {ref}`tweet` and {ref}`user` data named `tweets_v2` respectively `tweet_users`.

We also have view tables that make the data easier to supervise.

- `annotated_tweets`
- `extremist_tweets`
- `has_replies`
- `num_tweets_vs_actual_user_tweets`
- `user_tweets_status`
- `dataset`

## `PBWarehouse`

A service wrapper around a [PocketBase](https://pocketbase.io) instance and the [PocketBase-sdk](https://github.com/vaphes/pocketbase), providing methods for ingesting and retrieving `Tweet` and `User` records. It handles authentication, record creation, updates, and queries while adding validation and logging.

```{card}
:class-header: bg-light
:class-card: border-0 shadow-none

Parameters
^^^
`url` (*str*)
:   The URL of the PocketBase instance to connect to.
```

```{card}
:class-header: bg-light
:class-card: border-0 shadow-none

Attributes
^^^
`url` (*str*)
:   The URL of the PocketBase instance to connect to.

`authenticated` (*bool* | [*AdminAuthResponse*](https://github.com/vaphes/pocketbase/blob/master/pocketbase/services/admin_service.py#L25))
:   Is `False` if connection fails else an `AdminAuthResponse` object.
```

```{card}
:class-header: bg-light
:class-card: border-0 shadow-none

Raises
^^^
`ClientResponseError` (*Exception*)
:   Raises `ConnectionError` if the [PocketBase](https://pocketbase.io) server cannot be reached.


```
### Import

```python
from warehouse.pb import PBWarehouse
```
---

### Public Methods

#### `get_dataset()`

Fetch the entire dataset from the PocketBase collection `"dataset"`.

Returns
: *list*[[Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py)] -- Returns the `dataset` collection.

---

#### `ingest_user()`

Ingest a {ref}`user` object into the `tweet_users` collection.


````{note}
Skips if the user already exists.
````

Parameters
:   `user` ({ref}`user`) -- The user to ingest from the staging warehouse to the data warehouse.

Returns:
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) -- Returns the record from the collection
:    *None* -- Returns `None` if if Exceptions occur.

Raises:
: `ClientResponseError` -- If the data already exists in the database.

---

#### `ingest_single_tweet()`

Ingest a {ref}`tweet` object into the `"tweets_v2"` collection.

````{note}
* Skips if the tweet already exists.
* Ensures `tweet.user_id` is present.
````

Parameters
:   `tweet` ({ref}`tweet`) — The tweet to ingest.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The created tweet record.
:   *None* — If tweet already exists, `user_id` is missing, or ingestion fails.

Raises
:   `ClientResponseError` — If PocketBase returns a validation error.

---

#### `ingest_tweet()`

Ingest both tweet and user data (raw dict) into `"tweets_v2"` and `"tweet_users"`.

Parameters
:   `tweet` (*dict*) — Raw tweet JSON data to ingest.

Returns
:   *dict*[*str*, [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py)] — A dictionary with keys:
\* `"record_tweet"` — The Tweet record, or `None` if skipped.
\* `"record_user"` — The User record, or `None` if skipped.

---

#### `update_has_fetched_replies()`

Mark a tweet’s `"fetched_replies"` field as `True`.

Parameters
:   `tweet_id` (*str*) — The ID of the tweet to update.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The updated tweet record.

---

#### `get_user_by_id()`

Fetch a user by `user_id` from `"tweet_users"`.

Parameters
:   `user_id` (*str*) — The user ID to look up.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The user record.

---

#### `get_user_by_username()`

Fetch a user by `username`.

Parameters
:   `username` (*str*) — The username to search for.
:   `strict` (*bool*, default=*True*) — If `True`, match exactly; if `False`, allow partial matches.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The user record matching the query.

---

#### `get_tweet_with_no_classification()`

Fetch the first tweet from the given collection where `"is_hateful"` is `NULL`.

Parameters
:   `collection` (*str*, default="tweets_v2") — The collection to search.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The first unclassified tweet.

---

#### `get_user_with_not_fetched_tweets()`

Fetch a user whose `status` is `"not fetched"` (or `NULL`).

Parameters
:   `less_than_k_tweets` (*int*, optional) — If provided, restrict results to users with fewer than `k` tweets.

Returns
:   [*Record*](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — A user record matching the criteria.

---

#### `does_user_exist()`

Check if a user exists in the PocketBase warehouse.

Parameters
:   `user_id` (*str* | *None*) — The user ID.
:   `username` (*str | *None*) — The username.
:   `strict` (*bool*, default=*True*) — If `True`, exact match on username; if `False`, partial matches allowed.

Returns
:   *bool* — `True` if the user exists, otherwise `False`.

Raises
:   `ValueError` — If neither `user_id` nor `username` is provided.

---

#### `get_tweet_by_id()`

Fetch a tweet by its `tweet_id`.

Parameters
:   `tweet_id` (*str* | *int*) — The tweet ID (must be a valid decimal string if passed as `str`).

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The tweet record matching the given ID.

Raises
:   `ValueError` — If `tweet_id` is not a string or valid decimal string.

---

### Private Helpers

#### `_ingest_record()`

Generic method to ingest a record into a PocketBase collection.

Parameters
:   `record` ({ref}`user` | {ref}`tweet`) — The record instance to ingest.
:   `model_name` (*str*) — The name of the model ("user" or "tweet").
:   `collection_name` (*str*) — The PocketBase collection name.

Returns
:   [Record](https://github.com/vaphes/pocketbase/blob/master/pocketbase/models/record.py) — The created record.
:   *None* — If the record already exists or ingestion fails.

Raises
:   `ClientResponseError` — If PocketBase validation fails.
:   `Exception` — For unexpected ingestion errors.

---

#### `_process_tweet()`

Preprocess raw tweet JSON into a `Tweet` model dictionary.

* Adds derived fields such as:

  * `has_blm_hashtag`
  * `status_link`
  * `retweet_status_id`
  * `quoted_status_id`
  * `community_note`

Parameters
:   `tweet` (*dict*) — The raw tweet JSON.

Returns
:   *dict*[*str*, *any*] — A dictionary representation of the validated {ref}`tweet`.

Raises
:   `ValidationError` — If the tweet does not conform to the {ref}`tweet` model.

---

#### `_process_user()`

Extract and preprocess the user sub-dictionary from a tweet into a `User` model dictionary.

Parameters
:   `tweet` (*dict*) — The raw tweet JSON containing a `"user"` key.

Returns
:   *dict*[*str*, *any*] — A dictionary representation of the validated {ref}`user`.

Raises
:   `ValidationError` — If the user data does not conform to the {ref}`user` model.

---

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
