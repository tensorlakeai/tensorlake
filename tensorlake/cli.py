
import click

@click.group()
def tensorlake():
    pass

@click.command()
def deploy():
    """Prepare and deploy a graph to tensorlake."""
    click.echo('Deploying...')

@click.command()
def prepare():
    click.echo('Preparing...')

tensorlake.add_command(deploy)
tensorlake.add_command(prepare)

if __name__ == "__main__":
    tensorlake()
