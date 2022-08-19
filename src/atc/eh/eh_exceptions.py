from atc.atc_exceptions import AtcException


class AtcEhException(AtcException):
    pass


class AtcEhInitException(AtcEhException):
    pass


class AtcEhNoDataException(AtcEhInitException):
    pass


class AtcEhLogicException(AtcEhException):
    pass
