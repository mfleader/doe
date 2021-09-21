import itertools
import json
import sys
from pathlib import Path
import random

from kubernetes import client, config
import pandas as pd

import ryaml


def unchanged_levels(factor_levels):
    return (
        dict(filter(lambda item: not isinstance(item[1], list) or len(item[1]) == 1, factor_levels.items())),
        dict(filter(lambda item: isinstance(item[1], list) and len(item[1]) > 1, factor_levels.items()))
    )


def randomize_powerset(factor_levels):
    random.seed(1)
    power_set_levels = list(itertools.product(*factor_levels.values()))
    # random.shuffle(power_set_levels)
    # print(power_set_levels)
    df0 = pd.DataFrame.from_records(power_set_levels, columns=factor_levels.keys())
    # print(df0.sample(frac=1,random_state=1))
    for exp_levels in df0.to_dict('records'):
        yield exp_levels


def create_pod(env):
    return client.V1Pod(
        api_version = "v1",
        kind = "Pod",
        metadata = client.V1ObjectMeta(
            name = env['name'],
            namespace = env['namespace']
        ),
        spec = client.V1PodSpec(
            containers = [
                client.V1Container(
                    name = env['container']['name'],
                    image = env['container']['image'],
                    image_pull_policy = 'Always',
                    command = env['container']['command'],
                    args = env['container']['args']
                )
            ],
            restart_policy = 'Never'
        )
    )


def serialize_command_args(cmd_args: dict):
    for key, val in cmd_args.items():
        yield '--' + str(key)
        yield str(val)


def main():

    factor_levels_filepath = Path(sys.argv[1])
    with open(factor_levels_filepath, 'r') as factor_levels_file:
        factor_levels_dict = json.load(factor_levels_file)

    env = {
        'name': 'dnsperf-test-wrapper',
        'namespace': 'dnsperf-test',
        'container': {
            'name': 'container',
            'image': 'quay.io/mleader/dnsdebug:latest',
            'command': ['/bin/sh', '-c'],
            'args': None
        }
    }
    base_arg = [
        './dnsdebug/snafu-dnsperf.sh',
    ]

    print(unchanged_levels(factor_levels_dict))


    # for trial in randomize_powerset(factor_levels_dict):
    #     args = [
    #         *base_arg,
    #         *list(serialize_command_args(trial))
    #     ]
        # mypod = create_pod(env)
        # print(ryaml.dumps(client.ApiClient().sanitize_for_serialization(mypod)))




if __name__ == '__main__':
    main()
