import subprocess
import sys
import time
import shutil
from pathlib import Path
import os
import doe


def main():
    experiment_factor_levels_path = sys.argv[1]
    es_url = sys.argv[2]
    sdn_kubeconfig_path = Path(sys.argv[3])
    ovn_kubeconfig_path = Path(sys.argv[4])
    sleep_t = sys.argv[5]
    block_id = sys.argv[6]

    doe.main(
        factor_levels_filepath=experiment_factor_levels_path,
        es_url=es_url,
        es_index='snafu-dns',
        block_id=block_id
    )

    env = {
        'KUBECONFIG': sdn_kubeconfig_path
    }

    for ocp_app_yaml in Path('ocp_apps').iterdir():
        print(f"{ocp_app_yaml}")

        factor_levels = { item for item in ocp_app_yaml.stem.split('-')}
        if 'OpenShiftSDN' in factor_levels:
            env['KUBECONFIG'] = sdn_kubeconfig_path
        elif 'OVNKubernetes' in factor_levels:
            env['KUBECONFIG'] = ovn_kubeconfig_path
        print(env['KUBECONFIG'])

        subprocess.run(
            ['oc', 'apply', '-f', str(ocp_app_yaml)],
            env=env
        )
        subprocess.run(
            ['oc', 'delete', '-f', str(ocp_app_yaml)],
            env=env
        )
        time.sleep(sleep_t)
    shutil.rmtree('ocp_apps')
    os.makedirs('ocp_apps')


if __name__ == '__main__':
    main()