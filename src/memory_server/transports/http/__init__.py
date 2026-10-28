"""Optional HTTP transport. Import the factory lazily; native paths stay lean."""


def create_http_app(config, dependencies):
    from .app import create_http_app as build
    return build(config, dependencies)
