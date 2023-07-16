def remove_protected_attrs(item: dict) -> dict:
    """
    Remove protected (AWS-only) attributes from a DynamoDB item.
    """
    attrs = [attr for attr in item.keys() if attr.startswith('aws:')]
    for attr in attrs:
        del item[attr]
    return item
