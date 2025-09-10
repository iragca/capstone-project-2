# Architectures

These classes encapsulate the architecture of the Graph Neural Network that we are going to use. Thanks to [PyTorch](https://pytorch.org/), auto-differentiation and preimplemented layer architectures are already provided, simplifying model building. We only need to specify the loss/cost function used, layers and their shapes (units), and activations.

```{eval-rst}
.. autoclass:: src.architectures.HomoGNN
    :members:
    :undoc-members:
    :show-inheritance:
```

```{eval-rst}
.. autoclass:: src.architectures.HeteroGNN
    :members:
    :undoc-members:
    :show-inheritance:
```
