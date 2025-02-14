rmdir /S /Q dist

python setup.py sdist bdist_wheel

py -m twine upload dist/*
