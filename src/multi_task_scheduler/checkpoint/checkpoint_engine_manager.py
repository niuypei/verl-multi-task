"""Native checkpoint coordinator extension without new membership or sync logic."""

from verl.checkpoint_engine.base import CheckpointEngineManager


class MultiTaskCheckpointEngineManager(CheckpointEngineManager):
    """Trainer creates this ordinary object; all weight synchronization stays native."""
