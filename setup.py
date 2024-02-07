from setuptools import setup, find_packages
from os import path

HERE = path.abspath(path.dirname(__file__))
PROJECT = 'lookyloo'

with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

with open(path.join(HERE, PROJECT, 'VERSION'), encoding='utf-8') as f:
    VERSION = f.read().strip()

extras = {
    'dev': ['pytest'],
}
all_deps = set()
for _, deps in extras.items():
    for dep in deps:
        all_deps.add(dep)
extras['all'] = list(all_deps)

setup(
    name=PROJECT,
    version=VERSION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.9, <4',
    install_requires=[
        'astropy>=5.1',
    ],
    package_data={  # Optional
        PROJECT: ['VERSION'],
    },
    extras_require=extras,
    entry_points={
        'console_scripts': [
            f'lookyloo={PROJECT}.commands.lookyloo:main',
            f'lookyloo_watch={PROJECT}.commands.lookyloo_watch:main',
            f'lookyloo_bundle={PROJECT}.commands.lookyloo_bundle:main',
            f'lookyloo_spans={PROJECT}.commands.lookyloo_spans:main',
        ],
    },
    project_urls={  # Optional
        'Bug Reports': f'https://github.com/magao-x/{PROJECT}/issues',
    },
)
