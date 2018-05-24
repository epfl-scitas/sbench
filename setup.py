from setuptools import setup

setup(
    name='SCITAS Osu Benchmarks',
    version='0.1',
    py_modules=['sosu'],
    install_requires=[
        'Click',
        'Jinja2'
    ],
    entry_points='''
        [console_scripts]
        sosu=sosu:sosu
    '''
)
