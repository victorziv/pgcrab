from setuptools import setup

setup(
    name='pgcrab',
    version='0.0.1',
    description='Helps to put the sea food ingredients into a PosgreSQL DB',
    author='Eugene H. Krabs',
    author_email='mrkrabs@bottomup.com',
    license='MIT',
    packages=['pgcrab'],
    zip_safe=False,
    install_requires=[
        'psycopg2',
    ],
)
