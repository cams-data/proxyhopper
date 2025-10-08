import logging
import sys
import click
from aiohttp import web
from ._config import ProxyhopperConfig
from ._server import DispatcherServer

@click.group()
def cli():
    """Proxyhopper CLI"""

@cli.command()
@click.option('--port', default=8000, help='Port to run the server on.')
@click.option('--host', default='localhost', help='Host to bind the server to.')
@click.option('--config', default='proxyhopper.yaml', help='Path to YAML config file.')
@click.option('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).')
@click.option('--log-output', default='stdout', type=click.Choice(['stdout', 'file']), help='Logging output destination.')
def run(port, host, config, log_level, log_output):
    """Start the proxyhopper dispatcher server."""
    cfg = ProxyhopperConfig.from_yaml(config)
    server = DispatcherServer(cfg, log_level=log_level, log_output=log_output)
    app = server.create_app()
    web.run_app(app, host=host, port=port)

# for `python -m proxyhopper_dispatcher`
def main():
    cli()
