from asyncio import run

import polars as pl
import torch
from fastapi import FastAPI, HTTPException

from src.architectures import HomoGNN
from src.config import PROJECT_ROOT
from src.data import DatasetLoader, GraphBuilder, Preprocessor
from src.db import PBWarehouse
from src.inference import InferenceEngine
from src.models import Features, Graph, User
from src.models.backend import (
    GetUserAPIResponse,
    InferenceAPIResponse,
    InferenceRequest,
)
from src.scraper import TweetyScraper
from src.service import UserService

app = FastAPI()
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def load_model():
    model_path = PROJECT_ROOT / "best_model.pth"
    # NOTE: Make sure to adjust this whenever changing model architecture
    model = HomoGNN(input_size=5, hidden_size=8, num_layers=2).to(DEVICE)
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
user_service = UserService(pb, scraper)
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


@app.get("/")
def read_root():
    return {"message": "Welcome to the MVP3 Backend!"}


@app.post("/inference", response_model=InferenceAPIResponse)
def inference(request: InferenceRequest):
    engine = InferenceEngine(model=model, graph=graph, gb=gb, pb=pb)

    user = user_service.get_or_fetch_user(
        username=request.username,
        user_id=request.user_id,
        strict_matching=request.options.strict_matching,
    )
    list_of_scored_tweets = engine(
        user=user,
        top_k=request.options.top_k,
        descending=request.options.descending,
    )

    return {
        "message": "Inference request received",
        "data": list_of_scored_tweets,
        "user": user,
    }


@app.get("/user/{username}", response_model=GetUserAPIResponse)
async def get_user(username: str):
    try:
        user_record = pb.get_user_by_username(username)
        user = User(**user_record.__dict__) if user_record else None
    except Exception:
        user = run(scraper.get_user_info(username=username))
        pb.ingest_user(user)

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": "User found", "data": user}


@app.get("/health")
def health_check():
    return {"status": "healthy"}
