from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r') as req_file:
        return req_file.read().splitlines()

setup(
    name='uainepydat',
    version='0.9.1',
    author='Daniel Stamer-Squair',
    author_email='uaine.teine@hotmail.com',
    description='A python package to assist in data and database handling',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/uaineteine/uaine.pydat',  
    packages=["uainepydat"],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
    install_requires=read_requirements(),
    package_data={'': ['LICENSE']}  # Specify the license file here
)
