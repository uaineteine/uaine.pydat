import os

def list_files(startpath):
    # Read .gitignore and create a list of ignored paths
    with open('.gitignore', 'r') as f:
        ignored = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    # Add .git and .vscode to the list of ignored paths
    ignored.append('.git')
    ignored.append('.vscode')

    for root, dirs, files in os.walk(startpath):
        # Skip directories and files that are in .gitignore
        dirs[:] = [d for d in dirs if os.path.relpath(os.path.join(root, d), startpath) not in ignored]
        files = [f for f in files if os.path.relpath(os.path.join(root, f), startpath) not in ignored]

        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}|-- {}'.format(subindent, f))

# use the current working directory as the start path
list_files(os.getcwd())
