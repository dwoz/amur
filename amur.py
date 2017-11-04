'''
amur.py

context    The context migrations will be run in; contains base configuration and
           runtime specific overrides.

migrator    A callable that a step's statements and the context will be passed too.
		    When called the migrator is passed a 'statement' and 'context' as
            arguments.

step        A step has a name, a pointer to the last step, and an ordered array of
            statments for both upgrades and downgrades.

statement   A statment is an arbitrary string that will be passed to a migrator
            with the context when a migration step is executed.
'''
import os
import yaml
from collections import namedtuple


class NotFound(Exception):
    'raised when the requested migration is not found'


class Step(namedtuple('Step', 'name, next, next_step, last, last_step')):
    pass


def load_migrations(path):
    steps = {}
    root = None
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            with open(os.path.join(dirname, filename)) as fp:
                step = yaml.load(fp)
            if not step['last']:
                if root:
                    raise Exception("Multiple roots found")
                else:
                   root = step
                continue
            if step['last'] in steps:
                raise Exception("Duplicate step name")
            steps[step['last']] = step
    step = root
    while step:
        last_step = step
        step = steps.pop(step['name'], None)
        if step:
            last_step['next_step'] = step
            step['last_step'] = last_step
    return root


def iter_steps(root, start_at='', stop_at='', direction='up'):
    if direction == 'up':
        next_method = 'next_step'
    elif direction == 'down':
        next_method = 'last_step'
    else:
        raise Exception("direction must be 'up' or 'down'")
    if start_at:
        step = root
        while step:
            root = step
            if step['name'] == start_at:
                break
            step = step.get('next_step', None)
        else:
            raise NotFound(stop_at)
    elif direction == 'down':
        step = root
        while step:
            root = step
            step = step.get('next_step', None)
    step = root
    while step:
        yield step
        if step['name'] == stop_at:
            break
        step = step.get(next_method, None)
    else:
        if stop_at:
            raise NotFound(stop_at)


def list_migrations(root, start_at='', stop_at='', direction='up', **kwargs):
    steps = []
    for step in iter_steps(root, start_at, stop_at, direction):
        steps.append(step)
    return steps


def run_migrations(root, ctxt, migrator, start_at='', stop_at='', direction='up', **kwargs):
    _ctxt = kwargs.get('_base_ctxt', {}).copy()
    _ctxt.update(ctxt)
    migrator.before_migrations(start_at, stop_at, direction, root)
    for step in iter_steps(root, start_at, stop_at, direction):
        for stmt in step[direction]:
            migrator(stmt, _ctxt)
    migrator.after_migrations(start_at, stop_at, direction, root, step)


class KeyspaceMigrator(object):

    def __init__(self, session):
        self.session = session

    def __call__(self, stmt, ctxt, *args, **kwargs):
        print('execute: {}'.format(repr(stmt.format(**ctxt))))
        self.session.execute(stmt.format(**ctxt), [])

if __name__ == '__main__':
    # sudo ifconfig lo0 alias 127.0.0.2 up
    # sudo ifconfig lo0 alias 127.0.0.3 up
    # ccm create dev --nodes 3 -v 3.9 --start
    from persist import setup_cluster, KeyspaceMigrator
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy, TokenAwarePolicy
    hosts = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
    port = 9042
    username = 'cassandra'
    password = 'cassandra'
    cluster = Cluster(
        hosts,
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        port=port,
        auth_provider=PlainTextAuthProvider(username, password),
    )
    session = cluster.connect()
    migrator = KeyspaceMigrator(session)
    step = load_migrations('migrations')
    run_migrations(step, ctxt, migrator)
