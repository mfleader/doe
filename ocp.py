from dataclasses import dataclass
import re
import os

import anyio


@dataclass
class Machineset:
    name: str
    desired: int
    current: int
    age: str
    ready: int = None
    available: int = None


class OCP:
    def __init__(self, kubeconfig_path, network_type):
        self.kubeconfig = kubeconfig_path
        self.network_type = network_type
        self.workers = filter(lambda w: 'worker' in w.name, self.list_machinesets())

    async def list_machinesets(self) -> list[Machineset]:
        machinesets_table_output = await anyio.run_process([
            'oc', 'get', 'machinesets', '-n', 'openshift-machine-api'
        ])
        return parse_machineset_table(machinesets_table_output)

    async def scale_workers(self, node_quantity: int):
        for worker in self.workers:
            result = await anyio.run_process([
                'oc', 'scale', f'--replicas={node_quantity}', 'machineset', worker.name,
                '-n', 'openshift-machine-api'
            ])
            print(result.stdout.decode())


def parse_machineset_table(machinesets: str):
    return [
       parse_machineset_row(line) for line in machinesets.split("\n")[1:-1]
    ]


def parse_machineset_row(row: str):
    cells = re.split(r"\s{1,}", row)
    if len(cells) == 4:
        return Machineset(
            name = cells[0],
            desired = int(cells[1]),
            current = int(cells[2]),
            age = cells[3]
        )
    elif len(cells) == 6:
        return Machineset(
            name = cells[0],
            desired = int(cells[1]),
            current = int(cells[2]),
            ready = int(cells[3]),
            available = int(cells[4]),
            age = cells[5]
        )

from pprint import pprint


if __name__ == '__main__':
    with open('machineset_table.txt', 'r') as machinesets_file:
        lines = machinesets_file.read()
        pprint(parse_machineset_table(lines))
