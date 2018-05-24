from setuptools import setup, find_packages

setup(
    name='sbench',
    version='0.1',
    py_modules=['sbench'],
    packages=find_packages(include=['sbench']),
    pacjage_data={
      'sbench': ['templates/*']
    },
    install_requires=[
        'Click',
        'Jinja2',
        'SQLAlchemy'
    ],
    entry_points='''
        [console_scripts]
        sbench=sbench.commands:sbench
    '''
)
