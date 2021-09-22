import itertools
import json
import sys
from pathlib import Path
import random
import uuid
import toml

from kubernetes import client, config
import pandas as pd

import ryaml


def unchanged_levels(factor_levels):
    one_level = {}
    for key,val in factor_levels.items():
        if not isinstance(val, list):
            one_level[key] = val
        elif len(val) == 1:
            one_level[key] = val[0]
    return (
        one_level,
        dict(filter(lambda item: isinstance(item[1], list) and len(item[1]) > 1, factor_levels.items()))
    )


def randomize_powerset(factor_levels):
    random.seed(1)
    power_set_levels = list(itertools.product(*factor_levels.values()))
    # random.shuffle(power_set_levels)
    # print(power_set_levels)
    df0 = pd.DataFrame.from_records(power_set_levels, columns=factor_levels.keys())
    df0['trial_id'] = df0.index + 1
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


def main(
    es_url, es_index,
    factor_levels_filepath = 'ocp_apps',
    block_id = 1,
):
    # elasticsearch_url = sys.argv[2]
    # with open(factor_levels_filepath, 'r') as factor_levels_file:
    factor_levels_dict = toml.load(factor_levels_filepath)
    # print(factor_levels_dict)
    #     factor_levels_dict = json.load(factor_levels_file)

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

    base_args, factor_levels = unchanged_levels(factor_levels_dict)
    base_args_levels = [
        './dnsdebug/snafu_dnsperf.py',
        '--run-id',
        str(uuid.uuid4()),
        '--block-id',
        block_id,
        *list(serialize_command_args(base_args))
    ]

    # print(base_args_levels)

    for trial in randomize_powerset(factor_levels):
        env['container']['args'] = [
            *base_args_levels,
            *list(serialize_command_args(trial))
        ]
        print(env['container']['args'])
        # mypod = create_pod(env)
        # with open(f"ocp_apps/{'-'.join(trial.values())}.yaml", 'w') as ocp_app:
        #     ocp_app.write(ryaml.dumps(client.ApiClient().sanitize_for_serialization(mypod)))




if __name__ == '__main__':
    main()
