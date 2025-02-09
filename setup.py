from setuptools import setup, find_packages

def read_requirements():
    with open('.github/workflows/pypack_requirements.txt', 'r') as req_file:
        return req_file.read().splitlines()

setup(
    name='arxivquery',
    version='1.0.5',
    author='Daniel Stamer-Squair',
    author_email='uaine.teine@hotmail.com',
    description='A simple wrapper for extracting Arxiv search results to a workable dataframe',
    url='https://github.com/uaineteine/ArxivAPI',  
    packages=["arxivapi"],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=read_requirements(),
    package_data={'': ['LICENSE']}  # Specify the license file here
)
