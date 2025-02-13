import os
from uainepydat import fileio

# Example usage
relative_directory = "../uainepydat"  # Replace with your relative directory path
python_files = fileio.list_files_of_extension(os.path.abspath(relative_directory), "py")
modules = [os.path.splitext(os.path.basename(filepath))[0] for filepath in python_files]

print("Python modules identified in the directory: " + relative_directory)
for file in modules:
    print(file)

#make auto modules
def rst_text(module_name):
    return ".. automodule:: " + module_name + "\n   :members:"
rst_lines = list(map(rst_text, modules))
#print(rst_lines)

#open pre-compile file and edit lines