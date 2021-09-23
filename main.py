import subprocess
import sys
import time
import shutil
from pathlib import Path
import os
from pprint import pprint

import typer
from kubernetes import client, config
from openshift.dynamic import DynamicClient
import ryaml

import doe


app = typer.Typer()





@app.command()
def main(
    # experiment_factor_levels_path: str = typer.Argument(...),
    # es_url: str = typer.Option(...),
    sdn_kubeconfig_path: str = typer.Option(...),
    ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(2),
    block_id: int = typer.Option(1)
):

    sdn_cluster = config.new_client_from_config(sdn_kubeconfig_path)
    ovn_cluster = config.new_client_from_config(ovn_kubeconfig_path)

    # pprint(sdn_cluster)
    # pprint(ovn_cluster)


    sdn_ocp = DynamicClient(sdn_cluster)
    sdn_ocp_pods = sdn_ocp.resources.get(api_version='v1', kind='Pod')

    sdn_ocp_pods.create(
        body = {
            'kind': 'Pod',
            'apiVersion': 'v1',
            'metadata': {
                'name': 'dnsperf-test-wrapper',
                'namespace': 'dnsperf-test'
            },
            'spec': {
                'containers': [
                    {
                        'name': 'container',
                        'image': 'quay.io/mleader/dnsdebug:latest',
                        'imagePullPolicy': 'Always',
                        'command': ['/bin/sh', '-c'],
                        'args': [
                            "python",
                            "./dnsdebug/snafu_dnsperf.py"
                            "--run-id",
                            "88ff59df-117c-46ac-ae4b-ea5962869387",
                            "--block-id",
                            "1",
                            "--load_limit",
                            "inf",
                            "--query_path",
                            "./dnsdebug/noerror.txt",
                            "--control_plane_nodes",
                            "3",
                            "--network_type",
                            "OpenShiftSDN",
                            "--transport_mode",
                            "tcp",
                            "--client_threads",
                            "1",
                            "--trial_id",
                            "8",
                            "--runtime-length",
                            "300"
                        ]
                    }
                ],
                'restartPolicy': 'Never'
            }
        }
    )

    for event in sdn_ocp_pods.watch(namespace='dnsperf-test'):
        # print(str(event['object']))
        e = ryaml.loads(str(event['object']))
        # print(e)
        print(e['ResourceInstance[Pod]']['metadata']['name'])
        print(e['ResourceInstance[Pod]']['status']['phase'])
        if e['ResourceInstance[Pod]']['status']['phase'] == 'Succeeded':
            print(e['ResourceInstance[Pod]']['status']['containerStatuses'][-1]['state']['terminated']['reason'])

    # for pod in sdn_ocp_pods.get(namespace='dnsperf-test').items:
    #     print(pod.status.phase)
    #     print(pod.status.containerStatuses)
        print('-----------------------')


    # doe.main(
    #     factor_levels_filepath=experiment_factor_levels_path,
    #     es_url=es_url,
    #     es_index='snafu-dns',
    #     block_id=block_id
    # )

    # env = {
    #     'KUBECONFIG': sdn_kubeconfig_path
    # }

    # for ocp_app_yaml in Path('ocp_apps').iterdir():
    #     print(f"{ocp_app_yaml}")

    #     factor_levels = { item for item in ocp_app_yaml.stem.split('-')}
    #     if 'OpenShiftSDN' in factor_levels:
    #         env['KUBECONFIG'] = sdn_kubeconfig_path
    #     elif 'OVNKubernetes' in factor_levels:
    #         env['KUBECONFIG'] = ovn_kubeconfig_path
    #     print(env['KUBECONFIG'])

    #     subprocess.run(
    #         ['oc', 'apply', '-f', str(ocp_app_yaml)],
    #         env=env
    #     )
    #     subprocess.run(
    #         ['oc', 'delete', '-f', str(ocp_app_yaml)],
    #         env=env
    #     )
    #     time.sleep(sleep_t)
    # shutil.rmtree('ocp_apps')
    # os.makedirs('ocp_apps')


if __name__ == '__main__':
    app()