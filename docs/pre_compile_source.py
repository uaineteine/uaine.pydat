import os
from uainepydat import fileio
from uainepydat import dataclean

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
pre_compile_path = "source/index.rst_pre"
pre_str = fileio.read_file_to_string(pre_compile_path)
post_str = dataclean.replace_between_tags(pre_str, "automodule", rst_lines)

post_compile_path = "source/index.rst"
#overwrite the index.rst now
with open(post_compile_path, "w") as text_file:
    text_file.write(post_str)
print("Updated rst file")