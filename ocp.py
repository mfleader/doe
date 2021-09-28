from dataclasses import dataclass
import re
import os

import anyio

import time

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
        # self.workers = filter(lambda w: 'worker' in w.name, self.list_machinesets())
        self.env = os.environ.copy()
        self.env['KUBECONFIG'] = kubeconfig_path
        self.allowed_workers = {'worker-us-west-2a', 'worker-us-west-2b', 'worker-us-west-2c'}
        self.workers = None

    async def list_machinesets(self):
        os.environ['KUBECONFIG'] = self.kubeconfig
        machinesets_table_output = await anyio.run_process(
            command = [
                'oc', 'get', 'machinesets', '-n', 'openshift-machine-api'
            ],
            env = self.env
        )
        return parse_machineset_table(machinesets_table_output.stdout.decode())

    async def get_workers(self):
        machinesets = await self.list_machinesets()
        workers = list()
        for machineset in machinesets:
            if any((name in machineset.name for name in self.allowed_workers)):
                workers.append(machineset)
        return workers

    async def scale_workers(self, node_quantity: int):

        workers = await self.get_workers()
        nodes = int(node_quantity / len(self.allowed_workers))

        for worker in workers:
            result = await anyio.run_process(
                command = [
                    'oc', 'scale', f'--replicas={nodes}', 'machineset', worker.name,
                    '-n', 'openshift-machine-api'
                ],
                env = self.env
            )
            print(result.stdout.decode())

        workers = await self.get_workers()
        while any((worker.current != worker.desired for worker in workers)):
            workers = await self.get_workers()

        res = await anyio.run_process(
            command = [
                'oc', 'get', 'machinesets', '-n', 'openshift-machine-api'
            ],
            env = self.env)
        print(res.stdout.decode())
        time.sleep(5)


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
