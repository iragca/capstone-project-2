(mvps)=
# MVPs

## MVP 1.0: CLI Tool

This MVP showcases the core idea of how inference will work in future MVPs and the final production build.

To run inference, simply run `uv run mvp.py --user <user_id>` or `uv run mvp.py --username <username>`. The results will be printed as a `list` of `tuple`'s that contain the PocketBase `Record` object alongside its score/probability, i.e `(<Record: 7a13kcka0>, 0.52019314)`.

## MVP 2.0: Streamlit

This MVP showcases the implementation of the core inference functionality with a basic user interface.

To first run inference, we need to run the Streamlit server first by running `uv run streamlit run streamlit_mvp.py`. This requires working connection to the PocketBase database.

## MVP 3.0

Coming soon.
