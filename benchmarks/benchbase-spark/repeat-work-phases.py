#!/usr/bin/env python3
import argparse
import copy
import xml.etree.ElementTree as element_tree
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Repeat a BenchBase serial work phase."
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, required=True)
    return parser.parse_args()


def repeat_work_phases(config_path, repetitions):
    if repetitions < 1:
        raise ValueError("repetitions must be a positive integer")

    tree = element_tree.parse(config_path)
    works = tree.getroot().find("works")
    if works is None:
        raise ValueError("BenchBase config does not contain <works>")

    phases = works.findall("work")
    if len(phases) != 1:
        raise ValueError(
            "query repetitions require exactly one <work> phase"
        )

    template = phases[0]
    for _ in range(repetitions - 1):
        works.append(copy.deepcopy(template))

    tree.write(config_path, encoding="utf-8", xml_declaration=True)


def main():
    args = parse_args()
    repeat_work_phases(args.config, args.repetitions)


if __name__ == "__main__":
    main()
