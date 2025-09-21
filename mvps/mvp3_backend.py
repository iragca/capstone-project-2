from asyncio import run
from typing import Optional

import polars as pl
import torch
from deepsnap.dataset import GraphDataset
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.architectures import HomoGNN
from src.config import PROJECT_ROOT
from src.data import DatasetLoader, GraphBuilder, InferenceResults, Preprocessor
from src.db import PBWarehouse
from src.models import Features, Graph, Tweet, User
from src.scraper import TweetyScraper

app = FastAPI()
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def load_model():
    model_path = PROJECT_ROOT / "best_model.pth"
    # NOTE: Make sure to adjust this whenever changing model architecture
    model = HomoGNN(input_size=5, hidden_size=24).to(DEVICE)
    model.load_state_dict(torch.load(model_path, map_location=DEVICE))
    return model


def load_data(_pb: PBWarehouse):
    dataset_loader = DatasetLoader(_pb)
    data = dataset_loader.load_dataset()
    preprocessor = Preprocessor(data=data)
    return preprocessor.preprocess()


model: HomoGNN = load_model()
pb: PBWarehouse = PBWarehouse()
scraper: TweetyScraper = TweetyScraper(previous_session=True)
data: pl.DataFrame = load_data(pb)

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


class InferenceOptions(BaseModel):
    top_k: Optional[int] = Field(
        10, description="Number of top similar tweets to return", minimum=1, maximum=50
    )
    strict_matching: Optional[bool] = Field(
        True, description="Whether to use strict matching for user lookup"
    )
    descending: bool = Field(
        True, description="Whether to sort results in descending order of similarity"
    )


class InferenceRequest(BaseModel):
    username: str = Field(
        ..., example="elonmusk", description="Twitter handle of the user", min_length=1
    )
    user_id: Optional[str] = Field(
        None, example="44196397", description="Twitter ID of the user", min_length=1
    )
    options: Optional[InferenceOptions] = InferenceOptions()


class Result(BaseModel):
    node: Tweet
    score: float


class Response(BaseModel):
    message: str
    data: Optional[list[Result]] = None


@app.get("/")
def read_root():
    return {"message": "Welcome to the MVP3 Backend!"}


@app.post("/inference", response_model=Response)
def inference(request: InferenceRequest):
    x_handle = request.username
    x_user_id = request.user_id
    top_k = request.options.top_k if request.options and request.options.top_k else 10
    strict_matching = (
        request.options.strict_matching
        if request.options and request.options.strict_matching
        else False
    )
    descending = request.options.descending if request.options else True

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

        async def fetch_user_info():
            user: User = await scraper.get_user_info(
                int(x_user_id) if x_user_id else None, x_handle
            )
            return user

        user = run(fetch_user_info())

        if user is None:
            raise HTTPException(
                status_code=404,
                detail="User not found. Please check the handle or user ID.",
            )

        pb.ingest_user(user)

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
        node_embeddings, edge_label_index = model(dataset[0].to(DEVICE))
        results = InferenceResults(
            graph=graph,
            node_embeddings=node_embeddings.cpu(),
            edge_label_index=edge_label_index.cpu(),
        )

        topk_results = results.get_top_k_similar_nodes_linked_to_user(
            user_id=int(user.user_id), descending=descending, label=0, k=top_k
        )

        return_data = []
        for node_id, probability in topk_results:
            tweet = Tweet(**pb.get_tweet_by_id(node_id).__dict__)
            tweet.user_id = User(**pb.get_user_by_id(tweet.user_id).__dict__)

            return_data.append(Result(node=tweet, score=probability))

    return {"message": "Inference request received", "data": return_data}


@app.get("/health")
def health_check():
    return {"status": "healthy"}
