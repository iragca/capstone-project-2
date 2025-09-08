# Database

[PocketBase](https://pocketbase.io/) will be used for storing the {ref}`tweet` and {ref}`user` data.

The instance we mainly use is self hosted and can be found on https://capstone.gari-homelab.party/_/#/.

## Tables

We have two main tables for both {ref}`tweet` and {ref}`user` data named `tweets_v2` respectively `tweet_users`.

We also have view tables that make the data easier to supervise.

- `annotated_tweets`
- `extremist_tweets`
- `has_replies`
- `num_tweets_vs_actual_user_tweets`
- `user_tweets_status`
