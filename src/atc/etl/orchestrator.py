from abc import abstractmethod
from typing import List, Union

from pyspark.sql.dataframe import DataFrame

from .extractor import Extractor, DelegatingExtractor
from .loader import Loader, DelegatingLoader
from .transformer import Transformer, DelegatingTransformer, MultiInputTransformer, DelegatingMultiInputTransformer


class Orchestration:
    @staticmethod
    def extract_from(extractor: Extractor):
        return OrchestratorBuilder(extractor)


class OrchestratorBuilderException(Exception):
    pass


class LogicError(OrchestratorBuilderException):
    pass


class Orchestrator:
    extractor: Union[Extractor, DelegatingExtractor]
    transformer: Union[Transformer, DelegatingTransformer, MultiInputTransformer, DelegatingMultiInputTransformer]
    loader: Union[Loader, DelegatingLoader]

    @abstractmethod
    def execute(self) -> DataFrame:
        pass


class NoTransformOrchestrator(Orchestrator):
    def __init__(self, extractor: Extractor, loader: Union[Loader, DelegatingLoader]):
        self.extractor = extractor
        self.loader = loader

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        return self.loader.save(df)


class SingleExtractorOrchestrator(Orchestrator):
    def __init__(self,
        extractor: Extractor,
        transformer: Union[Transformer, DelegatingTransformer],
        loader: Union[Loader, DelegatingLoader]
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)

class MultipleExtractorOrchestrator(Orchestrator):
    def __init__(
        self,
        extractor: DelegatingExtractor,
        transformer: Union[MultiInputTransformer, DelegatingMultiInputTransformer],
        loader: Union[Loader, DelegatingLoader]
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def execute(self) -> DataFrame:
        dataset = self.extractor.read()
        df = self.transformer.process_many(dataset)
        return self.loader.save(df)


class OrchestratorBuilder(Orchestrator):
    extractors: List[Extractor]
    transformers: List[Union[MultiInputTransformer, Transformer]]
    loaders: List[Loader]

    def __init__(self, extractor: Extractor):
        self.extractors = [extractor]
        self.transformers = []
        self.loaders = []

    def extract_from(self, extractor: Extractor) -> "OrchestratorBuilder":
        if self.transformers or self.loaders:
            raise OrchestratorBuilderException("Set all extractors first")
        self.extractors.append(extractor)
        return self

    def transform_with(self, transformer: Union[Transformer, MultiInputTransformer]) -> "OrchestratorBuilder":
        if self.loaders:
            raise OrchestratorBuilderException("Set all transformers before loaders")
        if (len(self.extractors) > 1 and len(self.transformers) == 0 and not isinstance(transformer, MultiInputTransformer)):
            raise OrchestratorBuilderException(
                "Multiple extractors require first transfromer to be MultiInputTransformer"
            )
        if (len(self.extractors) == 1 and not isinstance(transformer, Transformer)):
            raise OrchestratorBuilderException(
                "Single extractors require transfromers to be Transformer not MultiInputTransformer"
            )
        self.transformers.append(transformer)
        return self

    def load_into(self, loader: Loader) -> "OrchestratorBuilder":
        self.loaders.append(loader)
        return self

    def execute(self) -> DataFrame:
        return self.build().execute()

    def build(self) -> Orchestrator:
        # Calculate length for each extractors, transformers or loaders list
        le = len(self.extractors)
        lt = len(self.transformers)
        ll = len(self.loaders)

        if le == 0:
            raise OrchestratorBuilderException("There have be at least one extractor")
        if ll == 0:
            raise OrchestratorBuilderException("There have be at least one loader")

        # Create single or delegating oject verion of extractors, transformers or loaders based on count
        extractorOject = self.extractors[0] if le == 1 else DelegatingExtractor(self.extractors)
        if le == 1:
            transformerOject = self.transformers[0] if lt == 1 else DelegatingTransformer(self.transformers)
        if le > 1:
            transformerOject = self.transformers[0] if lt == 1 else DelegatingMultiInputTransformer(self.transformers)
        loaderOject = self.loaders[0] if ll == 1 else DelegatingLoader(self.loaders)

        # Create Orchestrator types based on extractors, transformers or loaders count
        if lt == 0:
            return NoTransformOrchestrator(extractorOject, loaderOject)
        elif le == 1:
            return SingleExtractorOrchestrator(extractorOject, transformerOject, loaderOject)
        elif le > 1:
            return MultipleExtractorOrchestrator(extractorOject, transformerOject, loaderOject)
        else:
            raise LogicError(
                f"No supported orchestrator for "
                f"{le} extractors, "
                f"{le} transformers "
                f"and {ll} loaders"
            )
