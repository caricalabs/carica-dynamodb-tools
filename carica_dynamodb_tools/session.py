import boto3.session
import botocore.config
import botocore.session


def boto_session(*args, **kwargs) -> boto3.session.Session:
    """
    Get a boto3 session configured for more retries.
    """
    botocore_session = botocore.session.Session()
    botocore_session_config = botocore.config.Config(
        retries={
            'mode': 'standard',
            'max_attempts': 20,
        },
        max_pool_connections=100,
    )
    botocore_session.set_default_client_config(botocore_session_config)
    return boto3.session.Session(*args, **kwargs, botocore_session=botocore_session)
