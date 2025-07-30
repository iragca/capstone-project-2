from asyncio import run

import streamlit as st
import torch
from deepsnap.dataset import GraphDataset

from src.architectures import HomoGNN
from src.config import PROJECT_ROOT
from src.data import DatasetLoader, GraphBuilder, InferenceResults, Preprocessor
from src.db import PBWarehouse
from src.models import Features, Graph, Tweet, User
from src.scraper import TweetyScraper
from src.ui.utils import show_tweet

st.set_page_config(
    page_title="MVP Streamlit App",
    page_icon=":rocket:",
    layout="centered",
)

st.title("MVP Streamlit App")


@st.cache_resource
def load_model():
    model_path = PROJECT_ROOT / "best_model.pth"
    model = HomoGNN(input_size=5, hidden_size=32)
    model.load_state_dict(torch.load(model_path))
    return model


@st.cache_resource
def get_pb():
    return PBWarehouse()


@st.cache_resource
def get_scraper():
    return TweetyScraper(previous_session=True)


@st.cache_resource
def load_data(_pb: PBWarehouse):
    dataset_loader = DatasetLoader(_pb)
    data = dataset_loader.load_dataset()
    preprocessor = Preprocessor(data=data)
    return preprocessor.preprocess()


model = load_model()
pb = get_pb()
scraper = get_scraper()
data = load_data(pb)

node_features = Features(
    tweet=[
        "favorite_count",
        # "retweet_count",
        # "bookmark_count",
        "reply_count",
        "quote_count",
        # "views",
        "source",
        "is_hateful",
    ],
    user=[
        # "favourites_count",
        # "follower_count",
        # "following_count",
        # "number_of_tweets",
        # "listed_count",
        # "is_blue_verified",
        "friends",
    ],
)


gb = GraphBuilder(data=data, node_features=node_features)
graph: Graph = gb.create_graph()


col1, col2 = st.columns(2)

with col1:
    x_handle = st.text_input(
        value=None,
        label="Enter a Twitter handle (without @):",
        placeholder="e.g., elonmusk",
    )
    if x_handle == "None":
        x_handle = None

    if isinstance(x_handle, str):
        if x_handle.isdigit():
            st.error("Please enter a valid Twitter handle.")
            st.stop()
        if len(x_handle) == 0:
            x_handle = None


    strict_matching = st.checkbox(
        label="Use strict matching for username",
        value=False,
    )

    descending = st.checkbox(
        label="Sort results in descending order",
        value=True,
    )

with col2:
    x_user_id = st.text_input(
        value=None,
        label="or Enter a user ID:",
        placeholder="e.g., 123456",
    )

    top_k = st.number_input(
        label="Number of top similar tweets to display",
        min_value=1,
        max_value=100,
        value=10,
        step=1,
    )

    if x_user_id == "None":
        x_user_id = None

    if isinstance(x_user_id, str):
        if not x_user_id.isdigit():
            st.error("Please enter a valid user ID.")
            st.stop()
        if len(x_user_id) == 0:
            x_user_id = None


if st.button(
    "Predict potential interactions with Extremist tweets",
    type="primary",
    use_container_width=True,
):
    user = None

    # Validate input: require either handle or user_id
    if not x_handle and not x_user_id:
        st.error("Please provide either a Twitter handle or a user ID.")
        st.stop()  # Stop execution here if no input

    user_exists = pb.does_user_exist(
        user_id=x_user_id, username=x_handle, strict=strict_matching
    )

    if user_exists:
        if x_user_id is not None:
            user_record = pb.get_user_by_id(x_user_id)
        else:
            user_record = pb.get_user_by_username(x_handle, strict=strict_matching)

        user = User(**user_record.__dict__)

    else:
        if x_user_id is not None:
            st.info(
                f"User {x_user_id} does not exist in the database. Fetching user info..."
            )

        if x_handle is not None:
            st.info(
                f"User {x_handle} does not exist in the database. Fetching user info..."
            )

        async def fetch_user_info():
            user: User = await scraper.get_user_info(
                int(x_user_id) if x_user_id else None, x_handle
            )
            if user is None:
                st.error("User not found. Please check the handle or user ID.")
                st.stop()
            return user

        user = run(fetch_user_info())

        pb.ingest_user(user)
        st.success(f"User {user.user_id} fetched successfully.")

    # Generate user vector and add node to graph
    user_vector = gb.get_features(user.model_dump(), "user")
    graph.add_node(
        int(user.user_id),
        node_label=3,
        node_feature=user_vector,
        node_type="user",
    )

    dataset = GraphDataset([graph], task="link_pred", edge_train_mode="disjoint")

    model.eval()
    with torch.no_grad():
        node_embeddings, edge_label_index = model(dataset[0])
        results = InferenceResults(
            graph=graph,
            node_embeddings=node_embeddings,
            edge_label_index=edge_label_index,
        )

        topk_results = results.get_top_k_similar_nodes_linked_to_user(
            user_id=int(user.user_id), descending=descending, label=0, k=top_k
        )

        topk_results = [
            (Tweet(**pb.get_tweet_by_id(node_id).__dict__), probability)
            for node_id, probability in topk_results
        ]

        for tweet, probability in topk_results:
            show_tweet(tweet, pb, probability=probability)
