from .consumer import HandlerType


HANDLERS: list[tuple[str, HandlerType]] = list()


def consume(mt_type: str):
    def decorator(func: HandlerType):
        HANDLERS.append((mt_type, func))
        return func
    return decorator

