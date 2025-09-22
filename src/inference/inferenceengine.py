from typing import List, Union

import torch
from deepsnap.dataset import GraphDataset

from src.architectures import HomoGNN
from src.data import GraphBuilder
from src.db import PBWarehouse
from src.inference import InferenceResults
from src.models import DiGraph, Graph, Tweet, TweetLinkProbability, User


class InferenceEngine:
    def __init__(
        self,
        model: HomoGNN,
        graph: Union[DiGraph, Graph],
        gb: GraphBuilder,
        pb: PBWarehouse,
    ):
        self.model = model
        self.graph = graph
        self.gb = gb
        self.pb = pb

    def __call__(
        self, user: User, descending: bool = True, top_k: int = 10
    ) -> List[TweetLinkProbability]:
        """
        Perform link prediction inference for a given user and hydrate results.

        This method adds the user as a node in the graph, computes node
        embeddings using the GNN model, and retrieves the top-k most
        probable tweet links associated with that user. For each predicted
        tweet node, the corresponding tweet and author information are
        fetched from the database and returned as hydrated objects.

        Parameters
        ----------
        user : User
            The user object for which to run inference.
        descending : bool, default=True
            Whether to sort predicted links in descending order of probability.
        top_k : int, default=10
            The number of top candidate tweet links to return.

        Returns
        -------
        List[TweetLinkProbability]
            A list of TweetLinkProbability objects containing the predicted
            tweets, their associated authors, and link prediction scores.

        Notes
        -----
        This method depends on both the :class:`~src.data.GraphBuilder` for feature extraction
        and the database :class:`~src.db.PBWarehouse` for hydrating tweet and user data.
        """
        user_vector = self.gb.get_features(user.model_dump(), "user")
        DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.graph.add_node(
            int(user.user_id),
            node_label=3,
            node_feature=user_vector,
            node_type="user",
        )

        dataset = GraphDataset(
            [self.graph], task="link_pred", edge_train_mode="disjoint"
        )

        self.model.eval()
        with torch.no_grad():
            node_embeddings, edge_label_index = self.model(dataset[0].to(DEVICE))
            results = InferenceResults(
                graph=self.graph,
                node_embeddings=node_embeddings.cpu(),
                edge_label_index=edge_label_index.cpu(),
            )

            topk_results = results.get_top_k_similar_nodes_linked_to_user(
                user_id=int(user.user_id), descending=descending, label=0, k=top_k
            )

            return_data = []
            for node_id, probability in topk_results:
                tweet = Tweet(**self.pb.get_tweet_by_id(node_id).__dict__)
                tweet.user_id = User(**self.pb.get_user_by_id(tweet.user_id).__dict__)

                return_data.append(TweetLinkProbability(tweet=tweet, score=probability))
            return return_data
