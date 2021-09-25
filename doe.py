import itertools
import random
import uuid
import toml

import pandas as pd


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
    random.shuffle(power_set_levels)
    df0 = pd.DataFrame.from_records(power_set_levels, columns=factor_levels.keys())
    df0['trial'] = df0.index + 1
    print(f"Total trials to run: {len(df0.index)}")
    for exp_levels in df0.to_dict('records'):
        yield exp_levels


def serialize_command_args(cmd_args: dict):
    for key, val in cmd_args.items():
        yield '--' + str(key.replace('_', '-'))
        yield str(val)


def main(
    factor_levels_filepath,
    block = 1,
):
    factor_levels_dict = toml.load(factor_levels_filepath)
    base_args, factor_levels = unchanged_levels(factor_levels_dict)
    base_args = {
        'run_id': str(uuid.uuid4()),
        'block': block,
        **base_args
    }

    for trial in randomize_powerset(factor_levels):
        yield {
            **base_args, **trial
        }


if __name__ == '__main__':
    main()
