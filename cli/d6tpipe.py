import click
import json
import d6tpipe

@click.group()
def main():
    pass


@main.command()
@click.option("--pipe", help="Pipe name", required=True)
@click.option("--profile", help="Profile name")
def create(pipe, profile):
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        result = d6tpipe.api.upsert_pipe(api, {'name': pipe})
        if len(result) == 2:
            print(json.dumps(result[1], indent=4, sort_keys=True))
    except Exception as e:
        click.echo(e)


@click.option("--pipe", help="Pipe name", required=True)
@click.option("--profile", help="Profile name")
@click.option("--preview", help="Preview", is_flag=True, default=False, show_default=True)
@main.command()
def pull(pipe, profile, preview):
    pipe_name = pipe
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipe = d6tpipe.Pipe(api, pipe_name)
        if preview:
            pipe.pull_preview()
        else:
            pipe.pull()
    except Exception as e:
        click.echo(e)


@click.option("--pipe", help="Pipe name", required=True)
@click.option("--profile", help="Profile name")
@click.option("--preview", help="Preview", is_flag=True, default=False, show_default=True)
@main.command()
def push(pipe, profile, preview):
    pipe_name = pipe
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipe = d6tpipe.Pipe(api, pipe_name)
        if preview:
            pipe.push_preview()
        else:
            pipe.push()
    except Exception as e:
        click.echo(e)


@click.group(name='list')
def list_pipes():
    pass


@click.option("--profile", help="Profile name")
@list_pipes.command(name='remote')
def list_remote(profile):
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipes = api.list_pipes()
        print(json.dumps(pipes, indent=4, sort_keys=True))
    except Exception as e:
        click.echo(e)


@click.option("--profile", help="Profile name")
@list_pipes.command(name='local')
def list_local(profile):
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipes = api.list_local_pipes()
        print(json.dumps(pipes, indent=4, sort_keys=True))
    except Exception as e:
        click.echo(e)

    
@click.group()
def scan():
    pass


@click.option("--pipe", help="Pipe name", required=True)
@click.option("--profile", help="Profile name")
@click.option("--no-cache", help="No Cache", is_flag=True, default=False, show_default=True)
@scan.command(name='remote')
def scan_remote(pipe, profile, no_cache):
    pipe_name = pipe
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipe = d6tpipe.Pipe(api, pipe_name)
        scan_result = pipe.scan_remote(cached=(not no_cache))
        print(json.dumps(scan_result, indent=4, sort_keys=True))
    except Exception as e:
        click.echo(e)


@click.option("--pipe", help="Pipe name", required=True)
@click.option("--profile", help="Profile name")
@scan.command(name='local')
def scan_local(pipe, profile):
    pipe_name = pipe
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        pipe = d6tpipe.Pipe(api, pipe_name)
        scan_result = pipe.scan_local()
        print(json.dumps(scan_result, indent=4, sort_keys=True))
    except Exception as e:
        click.echo(e)


@click.group()
def profiles():
    pass


@profiles.command(name='list')
def list_profiles():
    try:
        profile_list = d6tpipe.api.list_profiles()
        click.echo(profile_list)
    except Exception as e:
        click.echo(e)


@click.group()
def token():
    pass

@click.option("--token", help="Token string", required=True)
@click.option("--profile", help="Profile name")
@token.command(name='set')
def set_token(token, profile):
    try:
        api = d6tpipe.api.APIClient(profile=profile)
        api.setToken(token)
    except Exception as e:
        click.echo(e)


@click.option("--profile", help="Profile name")
@token.command(name='get')
def get_token(profile):
    try:
        config = d6tpipe.api.ConfigManager(profile=profile).load()
        if 'token' in config:
            click.echo(config['token'])
        else:
            click.echo('No token found')
    except Exception as e:
        click.echo(e)
    

# Top level commands
main.add_command(create)
main.add_command(pull)
main.add_command(push)
main.add_command(scan)
main.add_command(list_pipes)
main.add_command(profiles)

# Sub commands for list
scan.add_command(list_remote)
scan.add_command(list_local)

# Sub commands for scan
scan.add_command(scan_remote)
scan.add_command(scan_local)

# Sub commands for profiles
profiles.add_command(list_profiles)
profiles.add_command(token)
token.add_command(set_token)
token.add_command(get_token)

if __name__ == '__main__':
    main()

