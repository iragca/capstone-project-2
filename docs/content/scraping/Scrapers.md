# Scripts

We have two main modules for getting the data, [`RapidApiScraper`](RapidAPI.ipynb) uses a RapidAPI API to gather the tweet and user data. `TweetyScraper` is mainly used to get the user's friend count.

These scrapers are used to mainly get {ref}`tweet` and {ref}`user` information.

The tasks that are involved in scraping are made into small scripts, which can be found on [scripts/scraping.py](https://github.com/iragca/capstone-project-2/blob/master/scripts/scraping.py)

`````{admonition} How to use

To use these scripts, you run `uv run scraping <command_name> <args/kwargs>`. While replaceing the underscores `_` with a dash `-`.

````{card}
:class-header: bg-light
:class-card: border-0 shadow-none

**Example**
^^^

Given a script:

```python
def lets_go(args1, args2) -> ...:
    """
    Does something
    """
```
You run:
```bash
uv run scraping lets-go --args1 value --args2 value
```
````
`````



```{eval-rst}
.. autofunction:: src.scripts.scraping.tweety_login_once
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_info_of_users
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_user_tweets_v2
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_all_users_tweets_by_oldbird
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_all_users_tweets_by_tweety
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_replies
```

```{eval-rst}
.. autofunction:: src.scripts.scraping.get_from_oldbird
```
````
