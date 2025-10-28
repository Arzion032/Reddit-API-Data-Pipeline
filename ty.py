from folder_structure import FolderStructureGenerator

# List of folders to be ignored in the folder structure generation
folders_to_ignore = [
    "__pycache__",
    ".git",
    ".idea",
    ".venv",
    "dag_processor",
    "dag_id=reddit_api_data_pipeline",
    "output",
    "ty.py",
    ".vscode",
    "__init__.py"
]

# Generate the markdown representation of the folder structure
folder_structure_generator = FolderStructureGenerator(ignored_folders=folders_to_ignore)
folder_structure_md = folder_structure_generator.generate_folder_structure_md()

# Print the markdown representation of the folder structure
print(folder_structure_md)