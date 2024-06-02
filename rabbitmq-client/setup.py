from setuptools import setup, find_packages

VERSION = '0.0.1'
PACKAGE_NAME = 'rabbitmq-client'
DESCRIPTION = 'A wrapper for a RabbitMQ client'
with open('README.md', 'r', encoding='utf-8') as fh:
    LONG_DESCRIPTION = fh.read()
with open('requirements.txt', 'r', encoding='utf-8') as rf:
    EGG_MARK = '#egg='
    EXTRA_INDEX_URL_MARK = '--extra-index-url'
    REQUIREMENTS, DEPENDENCY_LINKS = list(), list()
    for line in rf.read().splitlines():
        # Ignore Comments
        if line.startswith(('#')):
            continue
        # Add git repositories to dependencies
        elif line.startswith(('-e git:', '-e git+', 'git:', 'git+')):
            line = line.lstrip('-e ')  # In case that is using "-e"
            if EGG_MARK in line:
                name = line[line.find(EGG_MARK) + len(EGG_MARK):]
                repo = line[:line.find(EGG_MARK)]
                REQUIREMENTS.append('{} @ {}'.format(name, repo))
                DEPENDENCY_LINKS.append(line)
        # Add custom repositories
        elif line.startswith((EXTRA_INDEX_URL_MARK)):
            line = line.lstrip(EXTRA_INDEX_URL_MARK + ' ')
            DEPENDENCY_LINKS.append(line)
        else:
            REQUIREMENTS.append(line)

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author='CERTH',
    author_email='author@email.com',
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://github.com/GeorgeGiannopoulos/clients/rabbitmq-client.git',
    platforms='Posix; MacOS X; Windows',
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=['wheel'] + REQUIREMENTS,
    dependency_links=DEPENDENCY_LINKS,
    extras_require={
        "dev": ['pytest>=7.2.1'],
    },
    python_requires='>=3.8.5',
    classifiers=[
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
