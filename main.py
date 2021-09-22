import subprocess
import sys
import time
from pathlib import Path

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

    # for ocp_app_yaml in Path('ocp_apps').iterdir():
    #     print(f"{ocp_app_yaml}")

    #     network_type = ocp_app_yaml.split('-')[0]
    #     if network_type == 'OpenShiftSDN':
    #         env['KUBECONFIG'] = sdn_kubeconfig_path
    #     elif network_type == 'OVNKubernetes':
    #         env['KUBECONFIG'] = ovn_kubeconfig_path

    #     subprocess.run(
    #         ['oc', 'apply', '-f', f"ocp_apps/{ocp_app_yaml}"],
    #         env=env
    #     )
    #     subprocess.run(
    #         ['oc', 'delete', '-f', f"ocp_apps/{ocp_app_yaml}"],
    #         env=env
        # )
        # time.sleep(sleep_t)


if __name__ == '__main__':
    main()