import sys
import os
import re

root_path = "../chapters"
dir_list = os.listdir(root_path)

chapter_numbers = [int(re.search(r"(\d+)", filename).group(1)) for filename in dir_list]
latest_chapter = max(chapter_numbers) + 1

final_file_name = f"chapter{latest_chapter}-{'_'.join(sys.argv[1:])}".lower()
#print(final_file_name)

def create_new_doc(file_name, root_path) -> str | None:
    final_file_name = file_name
    file_path = os.path.join(root_path, f"{final_file_name}.md")
    
    try:
        with open(file_path, "w") as f:
            f.write("[Go back](../README.md)")
        print(f"File '{file_path}' created successfully.")
        return file_path
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def add_to_toc(latest_chapter, chapter_name, generated_doc_name):
    file_path = "../README.md"
    content = f"\n- [Chapter {latest_chapter} - {chapter_name}](chapters/{generated_doc_name.split('\\')[-1]})"
    try:
        with open(file_path, "a") as f:
            f.write(content)
        print("Successfully appended new chapter to TOC")
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

document_name = create_new_doc(final_file_name, root_path)
if document_name:
    add_to_toc(latest_chapter, " ".join(sys.argv[1:]), document_name)
