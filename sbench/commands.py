import json
import os
import os.path
import subprocess
import uuid

import click
import jinja2
import sqlalchemy
import sqlalchemy.orm

from ._sql import Base

# TODO: move this to a configuration file?
mpi_stacks = [
    ('gcc/6.4.0', 'mvapich2'),
    ('gcc/6.4.0', 'openmpi'),
    ('gcc/7.3.0', 'mvapich2'),
    ('gcc/6.4.0', 'openmpi'),
    ('intel/18.0.2', 'intel-mpi')
]

targets_info = {
    'E5v2': {'ncores': 16, 'mem': [64, 256, 512]},
    'E5v3': {'ncores': 24, 'mem': [64]},
    'E5v4': {'ncores': 28, 'mem': [128, 256]},
    's6g1': {'ncores': 28, 'mem': [192]},
}

test_list = {
    'osu_bw': {
        'template': 'slurm_osu.sh',
        'subdir': 'pt2pt',
        'command': 'osu_bw',
        'configurations': [
            # (nnodes, ntasks)
            (1, 2),
            (2, 2)
        ],
    },
    'osu_bibw': {
        'template': 'slurm_osu.sh',
        'subdir': 'pt2pt',
        'command': 'osu_bibw',
        'configurations': [
            # (nnodes, ntasks)
            (1, 2),
            (2, 2)
        ]
    },
    'osu_latency': {
        'template': 'slurm_osu.sh',
        'subdir': 'pt2pt',
        'command': 'osu_latency',
        'configurations': [
            # (nnodes, ntasks)
            (1, 2),
            (2, 2)
        ]
    },
    'osu_alltoall': {
        'template': 'slurm_osu.sh',
        'subdir': 'collective',
        'command': 'osu_alltoall',
        'configurations': [
            # (nnodes, ntasks)
            (1, None),
            (2, None)
        ]
    },
    'osu_allreduce': {
        'template': 'slurm_osu.sh',
        'subdir': 'collective',
        'command': 'osu_allreduce',
        'configurations': [
            # (nnodes, ntasks)
            (1, None),
            (2, None)
        ]
    },
    'hpl': {
        'template': 'slurm_hpl.sh',
        'command': 'xhpl',
        'configurations': [
            # (nnodes, ntasks)
            (2, None)
        ]
    },

}


#: List of parsers
_parsers = {}

#: Lists of benchmarks preparators
_preparators = {}


def parser(name):
    def _decorator(cls):
        _parsers[name] = cls
        return cls
    return _decorator


def update_sql_db(root, session):
    """Updates the session passed as argument with information from the
    benchmark run at root.

    Args:
        root (path): directory containing the tests results
        session (session): SQLite session to be updated

    Returns:
        True if the session was updated, False if some error wa encountered
    """

    Base.metadata.create_all(session.bind)

    t = os.path.join(root, 'context.json')

    with open(t) as f:
        context = json.load(f)

    try:
        from . import slurm
        job = slurm.SlurmJob(root, context)
        job.update_sql_db(session)
        test = _parsers[context['name']](job, context)
        test.update_sql_db(session)
        session.commit()
    except Exception as e:
        click.echo(
            click.style('[WARNING]', fg='yellow', bold=True)
            + ' couldn\'t process directory: {0}'.format(root)
        )
        return False

    return True


def preparator(name):
    def _decorator(cls):
        _preparators[name] = cls
        return cls
    return _decorator


@click.group()
def sbench():
    """SCITAS osu-micro-bechmarks runner"""
    pass


@sbench.command()
@click.option('--tests', default=None, help='Tests to be run')
@click.option('--targets', default=None,
              help='Targets to which tests should be submitted')
@click.argument(
    'directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True)
)
def run(tests, targets, directory):
    """Runs the specified benchmark using slurm. Puts all relevant
    files in a tree starting from the directory passed in as a
    parameter.
    """

    context = {}

    tests = tests.split(',') if tests else list(test_list.keys())
    not_existing = [x for x in tests if x not in test_list]
    if not_existing:
        msg = 'couldn\'t find the following tests: {0}'.format(', '.join(not_existing))
        raise click.ClickException(msg)

    targets = targets.split(',') if targets else list(targets_info.keys())
    not_existing = [x for x in targets if x not in targets_info.keys()]
    if not_existing:
        msg = 'couldn\'t find the following targets: {0}'.format(
            ', '.join(not_existing))
        raise click.ClickException(msg)

    env = jinja2.Environment(
        loader=jinja2.PackageLoader('sbench', 'templates'))

    template = env.get_template('slurm_template.sh')

    for target in targets:
        context['target'] = target
        target_str = click.style('[{0}]'.format(target), fg='blue', bold=True)
        for compiler, mpi in mpi_stacks:
            context['compiler'] = compiler
            context['mpi'] = mpi
            stack_str = click.style('\t[{0}]\t[{1}]'.format(compiler, mpi),
                                    fg='green', bold=True)
            for test in tests:

                test_str = click.style('\t[{0}]'.format(test), fg='red')
                click.echo(target_str + stack_str + test_str)

                context['name'] = test
                if 'subdir' in test_list[test]:
                    context['subdir'] = test_list[test]['subdir']
                context['command'] = test_list[test]['command']
                context['test_template'] = test_list[test]['template']

                for nnodes, ntasks in test_list[test]['configurations']:

                    context['nnodes'] = nnodes
                    context['ntasks'] = ntasks

                    uid = str(uuid.uuid4())
                    test_directory = os.path.join(directory, uid)
                    batch_file = os.path.join(test_directory, 'slurm_batch.sh')

                    context['test_directory'] = test_directory
                    context['output_file'] = os.path.join(test_directory,
                                                          'run.%A.out')
                    context['error_file'] = os.path.join(test_directory,
                                                         'run.%A.err')
                    context['extra_directives'] = \
                        test_list[test].get('extra_directives', [])

                    os.makedirs(test_directory)

                    if test in _preparators:
                        bench_prep = _preparators[test](test_directory,
                                                        context)
                        bench_prep.prepare()

                    # TODO: This part needs to be made more general if we start
                    # TODO: supporting more than one runner

                    # Dump here context information, so that it can be accessed
                    # easily by post-processing commands
                    json_file = os.path.join(test_directory, 'context.json')
                    with open(json_file, 'w') as f:
                        json.dump(context, f)

                    # Instantiate a batch file for Slurm, then submit the job
                    sbatch_content = template.render(**context)
                    with open(batch_file, 'w') as f:
                        f.write(sbatch_content)

                    subprocess.call(['sbatch', '--parsable', batch_file], stdout=subprocess.PIPE)


@sbench.command()
@click.option(
    '--db', default=':memory:', help='The DB to be created or updated'
)
@click.option(
    '-v', '--verbose', is_flag=True, default=False, help='Activate verbosity'
)
@click.argument(
    'directory', type=click.Path(
        exists=True, file_okay=False, dir_okay=True, readable=True
    )
)
def collect(db, verbose, directory):
    """Collects the results of a previous run and stores them in a
    SQLite DB.
    """
    engine = sqlalchemy.create_engine('sqlite:///' + db, echo=verbose)
    session = sqlalchemy.orm.sessionmaker(bind=engine)()

    for root, dirs, files in os.walk(directory):
        # If a directory containing tests data was found, then parse the
        # output and collect it into a DB
        if not dirs and 'context.json' in files:
            update_sql_db(root, session)
