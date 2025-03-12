import sys

def update_version(file_path, current_version, new_version):
    with open(file_path, 'r') as file:
        content = file.read()

    # Strip newlines and whitespace from current_version
    current_version = current_version.strip()
    content = content.replace(current_version, new_version)

    with open(file_path, 'w') as file:
        file.write(content)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python update_version.py <current_version> <new_version> <file_path>")
        sys.exit(1)

    current_version = sys.argv[1]
    new_version = sys.argv[2]
    file_path = sys.argv[3]

    update_version(file_path, current_version, new_version)

    print(f"Version updated from {current_version} to {new_version} in {file_path}")
