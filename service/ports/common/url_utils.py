

def build_rabbit_connection_url(
        protocol: str,
        user: str,
        password: str,
        host: str,
        port: str,
        virtual_host: str
):
    host = f"{host}:{port}" if port else host
    connection_url = f"{protocol}://{user}:{password}@{host}/{virtual_host}"
    return connection_url
