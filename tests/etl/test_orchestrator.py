import unittest
from unittest.mock import MagicMock

from atc.etl import OrchestratorFactory, Orchestrator


class OrchestratorTests(unittest.TestCase):

    def test_execute_invokes_extractor_read(self):
        sut = self._create_sut()
        sut.execute()
        sut.extractor.read.assert_called_once()

    def test_execute_invokes_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        sut.transformer.process.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut() -> Orchestrator:
        return OrchestratorFactory.create(MagicMock(),
                                          MagicMock(),
                                          MagicMock())
