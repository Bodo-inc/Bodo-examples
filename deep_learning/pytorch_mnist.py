import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader
import bodo
import horovod.torch as hvd

# Example adapted from https://github.com/horovod/horovod/blob/master/examples/pytorch/pytorch_mnist.py

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x)


log_interval = 10

def train(model, train_loader, train_sampler, optimizer, epoch, cuda):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if cuda:
            data, target = data.cuda(), target.cuda()
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % log_interval == 0:
            print('[{}] Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                hvd.rank(), epoch, batch_idx * len(data), len(train_sampler),
                100. * batch_idx / len(train_loader), loss.item()))


def metric_average(val, name):
    tensor = torch.tensor(val)
    avg_tensor = hvd.allreduce(tensor, name=name)
    return avg_tensor.item()


def test(model, test_dataset, test_loader, cuda):
    model.eval()
    test_loss = 0.
    test_accuracy = 0.
    for data, target in test_loader:
        if cuda:
            data, target = data.cuda(), target.cuda()
        output = model(data)
        # sum up batch loss
        test_loss += F.nll_loss(output, target, size_average=False).item()
        # get the index of the max log-probability
        pred = output.data.max(1, keepdim=True)[1]
        test_accuracy += pred.eq(target.data.view_as(pred)).cpu().float().sum()

    # divide by number of examples in this worker's partition
    test_loss /= len(test_dataset)
    test_accuracy /= len(test_dataset)

    # Horovod: average metric values across workers
    test_loss = metric_average(test_loss, 'avg_loss')
    test_accuracy = metric_average(test_accuracy, 'avg_accuracy')

    # Horovod: print output only on first rank
    if hvd.rank() == 0:
        print('\nTest set: Average loss: {:.4f}, Accuracy: {:.2f}%\n'.format(
            test_loss, 100. * test_accuracy))


def deep_learning(X_train, y_train, X_test, y_test):
    print("[{}] X_train size is {}, y_train size is {}".format(bodo.get_rank(), len(X_train), len(y_train)))
    if hvd.is_initialized():  # ranks not using horovod (e.g. non-gpu ranks) skip
        # deep learning args
        seed = 42
        momentum = 0.5  # SGD momentum
        use_adasum = False  # use adasum algorithm to do reduction
        lr = 0.01  # learning rate
        gradient_predivide_factor = 1.0  # apply gradient predivide factor in optimizer
        lr_scaler = hvd.size() if not use_adasum else 1
        fp16_allreduce = False  # use fp16 compression during allreduce
        epochs = 3

        cuda = bodo.dl.is_cuda_available()
        torch.manual_seed(seed)
        if cuda:
            torch.cuda.manual_seed(seed)

        train_dataset = torch.utils.data.TensorDataset(torch.from_numpy(X_train).unsqueeze(1), torch.from_numpy(y_train))
        train_sampler = torch.utils.data.RandomSampler(train_dataset)
        train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=64, sampler=train_sampler)

        test_dataset = torch.utils.data.TensorDataset(torch.from_numpy(X_test).unsqueeze(1), torch.from_numpy(y_test))
        test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=1000)

        model = Net()

        # By default, Adasum doesn't need scaling up learning rate.
        lr_scaler = hvd.size() if not use_adasum else 1

        if cuda:
            # Move model to GPU
            model.cuda()
            # If using GPU Adasum allreduce, scale learning rate by local_size.
            if use_adasum and hvd.nccl_built():
                lr_scaler = hvd.local_size()

        # Horovod: scale learning rate by lr_scaler.
        optimizer = optim.SGD(model.parameters(), lr=lr * lr_scaler,
                              momentum=momentum)

        # Horovod: broadcast parameters & optimizer state.
        hvd.broadcast_parameters(model.state_dict(), root_rank=0)
        hvd.broadcast_optimizer_state(optimizer, root_rank=0)

        # Horovod: (optional) compression algorithm.
        compression = hvd.Compression.fp16 if fp16_allreduce else hvd.Compression.none

        # Horovod: wrap optimizer with DistributedOptimizer.
        optimizer = hvd.DistributedOptimizer(optimizer,
                                             named_parameters=model.named_parameters(),
                                             compression=compression,
                                             op=hvd.Adasum if use_adasum else hvd.Average,
                                             gradient_predivide_factor=gradient_predivide_factor)

        for epoch in range(1, epochs + 1):
            train(model, train_loader, train_sampler, optimizer, epoch, cuda)
            test(model, test_dataset, test_loader, cuda)

    bodo.barrier()


@bodo.jit
def main():
    X_train = np.fromfile("data/train_data.dat", np.uint8)
    X_train = X_train.reshape(60000, 28, 28)
    y_train = np.fromfile("data/train_targets.dat", np.int64)

    X_test = np.fromfile("data/test_data.dat", np.uint8)
    X_test = X_test.reshape(10000, 28, 28)
    y_test = np.fromfile("data/test_targets.dat", np.int64)

    # preprocessing: do image normalization in Bodo
    # https://pytorch.org/docs/stable/torchvision/transforms.html#torchvision.transforms.ToTensor
    # https://pytorch.org/docs/stable/torchvision/transforms.html#torchvision.transforms.Normalize
    # using mean=0.1307, std=0.3081
    X_train = ((X_train / 255) - 0.1307) / 0.3081
    X_train = X_train.astype(np.float32)
    X_test = ((X_test / 255) - 0.1307) / 0.3081
    X_test = X_test.astype(np.float32)

    X_train, y_train = bodo.dl.prepare_data(X_train, y_train)
    X_test, y_test = bodo.dl.prepare_data(X_test, y_test)
    with bodo.objmode:
        deep_learning(X_train, y_train, X_test, y_test)


main()
