def to_camel_case(string: str):
    return "".join(s.lower().capitalize() for s in string.split('-'))
