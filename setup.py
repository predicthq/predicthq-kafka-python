from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()


with open('README.md') as f:
    readme = f.read()

with open('VERSION') as f:
    version = f.read().strip()

setup(
    name='phq-kafka-python',
    version=version,
    description='Wrapper and utils around confluent-python-kafka',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='PredictHq',
    author_email='developers@predicthq.com',
    url='https://github.com/predicthq/predicthq-kafka-python',
    install_requires=requirements,
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ]
)
