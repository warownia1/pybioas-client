#!/usr/bin/env python

import os
import re
import sqlite3

import click
import requests
from requests.exceptions import RequestException

DOWNLOADS_DIR = 'downloads'


@click.group()
@click.option('--host', '-h', default='http://localhost:8080',
              show_default=True, prompt=True,
              help='A full URL to PyBioAS web server.')
@click.pass_context
def cli(ctx, host):
    ctx.obj['HOST'] = host


@cli.command(name='test')
@click.pass_context
def test_connection(ctx):
    """Test connection to the server."""
    click.echo('Testing server status...')
    try:
        response = requests.get(
            ctx.obj['HOST'] + '/echo',
            params={'test': 'pybioas-client'}
        )
    except RequestException:
        click.echo('No connection could be made.', err=True)
        raise ctx.abort()
    if response.status_code == 200:
        try:
            json_r = response.json()
            assert json_r['method'] == 'GET'
            assert json_r['args'] == {'test': 'pybioas-client'}
        except (ValueError, KeyError, AssertionError):
            click.echo('Invalid server response.', err=True)
            raise ctx.abort()
        else:
            click.echo("OK")
    else:
        return ctx.fail(
            "Server responded with status code %d" % response.status_code)


@cli.command()
@click.pass_context
def services(ctx):
    """List all available services."""
    try:
        response = requests.get(ctx.obj['HOST'] + '/services')
    except requests.exceptions.ConnectionError:
        click.echo('Connection error', err=True)
        raise ctx.abort()
    if response.status_code == 200:
        try:
            json_r = response.json()
        except ValueError:
            return ctx.fail('Received invalid JSON.', err=True)
    else:
        return ctx.fail(
            "Server responded with status code %d." % response.status_code)

    click.echo("Available services:")
    for service in json_r['services']:
        click.echo("  %s" % service)


@cli.command()
@click.argument('service')
@click.pass_context
def form(ctx, service):
    """Get the service form"""
    host = ctx.obj['HOST']
    try:
        url = "%s/service/%s/form" % (host, service)
        response = requests.get(url)
    except RequestException:
        click.echo("Connection error", err=True)
        raise ctx.abort()
    if response.status_code == 200:
        try:
            json_r = response.json()
        except ValueError:
            click.echo('Received invalid JSON.', err=True)
            raise ctx.abort()
    elif response.status_code == 404:
        click.echo('Service %s not found.' % service)
        raise ctx.abort()
    else:
        click.echo(
            'Server responded with status code %d.' % response.status_code,
            err=True
        )
        raise ctx.abort()

    click.echo(json_r['form'])
    click.echo('fields:')
    for field in json_r['fields']:
        s = [' - %s: %s' % (field['name'], field['type'])]
        if not field.get('required'):
            s.append("(optional)")
        if field.get('default') is not None:
            s.append("[default: %s]" % field['default'])
        click.echo(' '.join(s))


@cli.command()
@click.argument('service')
@click.argument('fields', nargs=-1)
@click.pass_context
def submit(ctx, service, fields):
    """Submits the form to the server."""
    host = ctx.obj['HOST']

    try:
        values, files = _parse_fields(fields)
    except ValueError as e:
        click.echo(e, err=True)
        raise ctx.abort()

    # upload files and fill file fields values
    uploader = FileUploader(host)
    try:
        uploader.upload([f[1] for f in files])
    except (RequestException, OSError, ValueError) as e:
        uploader.rollback()
        click.echo(e, err=True)
        raise ctx.abort()
    for (field, path) in files:
        values[field] = uploader.id(path)

    # send the request
    try:
        url = '%s/service/%s/form' % (host, service)
        response = requests.post(url, data=values)
    except RequestException:
        click.echo('Error: Connection error', err=True)
        raise ctx.abort()
    if response.status_code == 202:
        try:
            json_r = response.json()
        except ValueError:
            uploader.rollback()
            click.echo('Error: Received invalid JSON.', err=True)
            raise ctx.abort()
    elif response.status_code == 420:
        uploader.rollback()
        click.echo('Error: Form is not valid.', err=True)
        raise ctx.abort()
    else:
        uploader.rollback()
        click.echo(
            'Server responded with status code %d.' % response.status_code,
            err=True
        )
        raise ctx.abort()

    task_id = json_r['taskId']
    with DBConnection('sqlite.db') as conn:
        num = conn.insert_task(task_id)
    click.echo('%s (#%d)' % (task_id, num))


def _parse_fields(fields):
    files = list()
    values = dict()
    for field in fields:
        match = re.match(r'(\w+?)=(.+)', field)
        if match is None:
            raise ValueError('Error: Invalid argument %r' % field)
        value = match.group(2)
        if value.startswith('@'):
            files.append((match.group(1), value[1:]))
        else:
            values[match.group(1)] = value
    return values, files


@cli.command()
@click.argument('task_id')
@click.pass_context
def task(ctx, task_id):
    if task_id.startswith('#'):
        with DBConnection('sqlite.db') as conn:
            task_row = conn.select_task(task_id[1:])
            if task_row is None:
                click.echo("Task with id %s doesn't exist." % task_id)
                raise ctx.abort()
            task_id = task_row[1]
    host = ctx.obj['HOST']
    try:
        url = '%s/task/%s' % (host, task_id)
        response = requests.get(url)
    except RequestException as e:
        click.echo(e)
        raise ctx.abort()
    if response.status_code == 200:
        try:
            json_r = response.json()
        except ValueError:
            click.echo('Error: Received invalid JSON.', err=True)
            raise ctx.abort()
    else:
        click.echo(
            'Server responded with status code %d.' % response.status_code,
            err=True
        )
        raise ctx.abort()

    click.echo('Job status: %s\n' % json_r['status'])
    if json_r['ready']:
        working_dir = os.path.join(DOWNLOADS_DIR, task_id[:8])
        os.makedirs(working_dir, exist_ok=True)
        with open(os.path.join(working_dir, 'stdout.txt'), 'w') as f:
            f.write(json_r['output']['stdout'])
        with open(os.path.join(working_dir, 'stderr.txt'), 'w') as f:
            f.write(json_r['output']['stderr'])

        files = json_r['output'].get('files')
        if files:
            downloader = FileDownloader(host, working_dir)
            with click.progressbar(files) as bar:
                for file_id in bar:
                    try:
                        downloader.download(file_id)
                    except FileExistsError:
                        click.echo('File %s exists - skipping' % file_id)
                    except RequestException as e:
                        click.echo('Failed to download %s.' % file_id)
                        click.echo(e)
        click.echo('')
        click.echo('return: %d\n' % json_r['output']['returnCode'])
        click.echo('stdout: \n%s\n' % json_r['output']['stdout'])
        click.echo('stderr: \n%s\n' % json_r['output']['stderr'])


class FileDownloader:

    def __init__(self, host, download_dir):
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
        self._host = host
        self._download_dir = download_dir
        self._download_url = self._host + '/file/%s/download'

    def download(self, file_id):
        """
        Downloads specified file from the server.
        :param file_id: file identifier
        :return: path to the file
        :raise RequestException:
        :raise FileExistsError:
        """
        path = os.path.join(self._download_dir, file_id)
        if os.path.isfile(path):
            raise FileExistsError
        response = requests.get(self._download_url % file_id)
        with open(path, 'wb') as f:
            f.write(response.content)
        return path


class FileUploader:

    def __init__(self, host):
        self._host = host
        self._uploaded = dict()

    def upload(self, file_paths):
        """
        Uploads all files to the server.
        :raise RequestException:
        :raise FileNotFoundError:
        :raise ValueError: invalid JSON response
        """
        for path in file_paths:
            if not os.path.isfile(path):
                raise OSError('%s is not a file.' % path)
        total_size = sum(os.stat(path).st_size for path in file_paths)
        with click.progressbar(length=total_size, label='Uploading files') as bar:
            for path in file_paths:
                r = requests.post(
                    self._host + '/file',
                    data={'mimetype': 'text/plain'},
                    files={'file': open(path, 'rb')}
                )
                json_r = r.json()
                self._uploaded[path] = (json_r['id'], json_r['signedId'])
                bar.update(os.stat(path).st_size)

    def id(self, path):
        return self._uploaded[path][0]

    def rollback(self):
        """Rollbacks progress deleting all _uploaded files."""
        pass


class DBConnection:

    def __init__(self, file):
        if not os.path.isfile(file):
            self.create_new_db(file)
        self.conn = sqlite3.connect(file)

    def __enter__(self):
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @staticmethod
    def create_new_db(file):
        conn = sqlite3.connect(file)
        cur = conn.cursor()
        cur.execute("CREATE TABLE tasks "
                    "(id INTEGER PRIMARY KEY AUTOINCREMENT, task_id);")
        conn.commit()
        conn.close()

    def close(self):
        self.conn.rollback()
        self.conn.close()

    def insert_task(self, task_id):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO tasks(task_id) "
                    "VALUES(?)", (task_id,))
        self.conn.commit()
        return cur.lastrowid

    # noinspection PyShadowingBuiltins
    def select_task(self, id):
        cur = self.conn.cursor()
        cur.execute("SELECT id, task_id "
                    "FROM tasks "
                    "WHERE id=?", (id,))
        return cur.fetchone()


if __name__ == '__main__':
    cli(obj={})
