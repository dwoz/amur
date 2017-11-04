import os
import pytest

from amur import load_migrations


def remove_indent(s):
    idt = 0
    o = ''
    for i in s.split('\n'):
        if idt == 0:
            t = i.strip()
            idx = s.index(t)
            if idx > 0:
                idt = idx
        i = i[idt-1:]
        o += i + '\n'
    return o

def test_amur_a(tmpdir):
    migrations_path = os.path.join(tmpdir.strpath, 'migrations')
    assert not os.path.exists(migrations_path)
    assert load_migrations(migrations_path) == None


def test_amur_b(tmpdir):
    migrations_path = os.path.join(tmpdir.strpath, 'migrations')
    os.mkdir(migrations_path)
    step_path = os.path.join(migrations_path, 'step_one.yml')
    with open(step_path, 'w') as fp:
        fp.write(remove_indent('''
        name: migration_one
        last:
        up:
          - >
            up command
        down:
          - >
            down command
        '''))
    assert load_migrations(migrations_path) == {
        'name': 'migration_one',
        'last': None,
        'up': ['up command\n'],
        'down': ['down command\n'],
    }
