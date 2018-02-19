import zmq
import time
from multiprocessing import Process
import os

"""
An attempt to simulate IMPALA learning within pyzmq.
A set of actors processes wake up, initialise the start model via a common random seed (not shown here)
and start the following proceess:
 - Non-blocking check if any new models are available, if so, update the local one;
 - Play a game,
 - Publish the observed trajectory to the learner (non-blocking)
 - Repeat.

The learner's process:
 - Blocking wait to collect batch_size (10 here) trajectories,
 - "Train" a new model,
 - Publish (non-blocking) it to the learners,
 - Repeat

To demonstrate the robustness, actor-0 kills his own process after epoch 2.
"""

def actor(actor_id, model_port, trajectory_port):
    def _log(s):
        print("[actor-{}] {}".format(actor_id, s))

    context = zmq.Context()
    model_read_socket = context.socket(zmq.SUB)
    # reading model broadcasts
    model_read_socket.connect("tcp://localhost:{}".format(model_port))
    model_read_socket.setsockopt(zmq.SUBSCRIBE, b'')

    # sending trajectories updates
    trajectory_send_socket = context.socket(zmq.PUB)
    trajectory_send_socket.connect("tcp://localhost:{}".format(trajectory_port
        ))

    n_trajectories = 0
    model = {'epoch': 0}
    while True:
        if model['epoch'] == 2 and actor_id == 0:
            _log("Time has come, the suicide time - I'm killing myself for the testing purposes"
                )
            os.system('kill %d' % os.getpid())
        try:
            _log('Trying to get updated model')
            model = model_read_socket.recv_pyobj(flags=zmq.NOBLOCK)
            _log('Success')
        except zmq.Again:
            _log('No new model, continue using old one...')
        _log("Generating trajectory using model from epoch {}".format(model[
            'epoch']))
        time.sleep(2)
        n_trajectories += 1
        _log("Sending the trajectory over")
        trajectory = {"name": "trajectory",
               "trajectory_id": n_trajectories,
               "model_epoch_used": model['epoch'],
               "actor_id": actor_id}
        trajectory_send_socket.send_pyobj(trajectory)


def learner(model_port, trajectory_port):
    context = zmq.Context()
    model_send_socket = context.socket(zmq.PUB)
    model_send_socket.bind("tcp://*:{}".format(model_port))

    # broadcasting the models
    trajectory_read_socket = context.socket(zmq.SUB)
    trajectory_read_socket.bind("tcp://*:{}".format(trajectory_port))
    trajectory_read_socket.setsockopt(zmq.SUBSCRIBE, b'')

    epoch = 0
    trajectories_read = 0

    while True:
        print("[learner] Sending model...")
        data = {'name': 'model', 'epoch': epoch,
                'trajectories_seen': trajectories_read}
        model_send_socket.send_pyobj(data)
        epoch += 1

        print("[learner] Reading the trajectories")
        for _ in range(10):
            trajectory = trajectory_read_socket.recv_pyobj()
            print("\tGot a trajectory from actor_id {}".format(trajectory[
                'actor_id']))
            trajectories_read += 1
        print('[learner] Read {} trajectories in total'.format(
            trajectories_read))
        time.sleep(10)


if __name__ == "__main__":
    # Learner publishes models at port 2000 as the
    model_port = 2000
    # actors publish trajectories to
    # port 3000 on the learner
    trajectories_port = 3000

    Process(target=learner, args=(model_port, trajectories_port)).start()
    n_actors = 5
    for i in range(n_actors):
        Process(target=actor, args=(i, model_port, trajectories_port)).start()
