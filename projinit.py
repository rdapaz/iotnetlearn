from pathlib import Path
import yaml
import sys
import pprint
import time
import os


def pretty_printer(o):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(o)

current_dir = Path.cwd()
os.chdir(current_dir)

folder_structure = """
db:
    - db01
    - db02
    - db03
    - db04
    - db05
    - db06
    - db07
    - db08
    - db09
    - db10
bin:
    - archive
notebooks:
    - common
    - deep_learning
    - gradient_boost
    - random_forest
pcap_captures:
    - pcap01
    - pcap02
    - pcap03
"""

folders = yaml.load(folder_structure)
pretty_printer(folders)

for root_folder, sub_folders in folders.items():
    if not os.path.exists(root_folder):
        os.mkdir(root_folder)
        os.chdir(root_folder)
        for folder in sub_folders:
            os.mkdir(folder)
        os.chdir(current_dir)