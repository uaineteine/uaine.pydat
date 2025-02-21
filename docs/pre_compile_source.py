import sys
import os

# Append the relative directory to sys.path
sys.path.append(os.path.join("..", 'uainepydat'))
import fileio
import datatransform

relative_directory = "../uainepydat"  # Replace with your relative directory path
python_files = fileio.list_files_of_extension(os.path.abspath(relative_directory), "py")
modules = [os.path.splitext(os.path.basename(filepath))[0] for filepath in python_files]
#sort these modules out in alphabetical order
modules = sorted(modules)

print("Python modules identified in the directory: " + relative_directory)
for file in modules:
    print(file)

#make auto modules
def rst_text(module_name):
    rst = module_name + "\n=========================" + "\n"
    return rst + "\n.. automodule:: " + module_name + "\n   :members:" + "\n"
rst_lines = list(map(rst_text, modules))
#print(rst_lines)

#open pre-compile file and edit lines
pre_compile_path = "source/index.rst_pre"
pre_str = fileio.read_file_to_string(pre_compile_path)
post_str = datatransform.replace_between_tags(pre_str, "automodule", rst_lines, deleteTags=True)

#update the dependency list
requirements_path = "../requirements.txt"
#make these into dotpoints
requirements = fileio.read_file_to_string(requirements_path)
requirements = datatransform.break_into_lines(requirements)
requirements = list(map(lambda string: datatransform.add_prefix(string, "* "), requirements))
requirements.append("\n")
post_str = datatransform.replace_between_tags(post_str, "dependencies", requirements, deleteTags=True)

post_compile_path = "source/index.rst"
#overwrite the index.rst now
with open(post_compile_path, "w") as text_file:
    text_file.write(post_str)
print("Updated rst file")