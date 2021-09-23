import subprocess
import sys
import time
import shutil
from pathlib import Path
import os
from pprint import pprint

import typer
from kubernetes import client, config, watch
import ryaml

import doe


app = typer.Typer()


JOB_NAME = "dnsperf-test-wrapper"

from kubernetes.dynamic import DynamicClient
from kubernetes.client.models.v1_job_condition import V1JobCondition
from kubernetes.client.models.v1_job_status import V1JobStatus
from kubernetes.client.api.batch_v1_api import BatchV1Api


k8s_job_attribute_map = {
    val: key for key, val in V1JobStatus.attribute_map.items()
}

def create_job_object(job_args):
    # Configureate Pod template container
    container = client.V1Container(
        name=JOB_NAME,
        image='quay.io/mleader/dnsdebug:latest',
        command=["/bin/sh", "-c"],
        args=job_args)
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": JOB_NAME}),
        spec=client.V1PodSpec(restart_policy="Never", containers=[container]))
    # Create the specification of deployment
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=4)
    # Instantiate the job object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=JOB_NAME),
        spec=spec)

    return job


def create_job(api_instance, job):
    api_response = api_instance.create_namespaced_job(
        body=job,
        namespace="dnsperf-test")
    print("Job created. status='%s'" % str(api_response.status))


def update_job(api_instance, job):
    # Update container image
    job.spec.template.spec.containers[0].image = "perl"
    api_response = api_instance.patch_namespaced_job(
        name=JOB_NAME,
        namespace="dnsperf-test",
        body=job)
    print("Job updated. status='%s'" % str(api_response.status))


def delete_job(api_instance):
    api_response = api_instance.delete_namespaced_job(
        name=JOB_NAME,
        namespace="dnsperf-test",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Job deleted. status='%s'" % str(api_response.status))


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


@app.command()
def main(
    # experiment_factor_levels_path: str = typer.Argument(...),
    # es_url: str = typer.Option(...),
    sdn_kubeconfig_path: str = typer.Option(...),
    # ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(2),
    block_id: int = typer.Option(1)
):

    sdn_cluster = config.new_client_from_config(sdn_kubeconfig_path)
    # ovn_cluster = config.new_client_from_config(ovn_kubeconfig_path)

    myargs = [
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
                            "2"
                        ]



    # batch_v1 = sdn_cluster.BatchV1Api()

    batch_v1 = BatchV1Api(api_client=sdn_cluster)
    job = create_job_object(myargs)
    sdn_dynamic = DynamicClient(sdn_cluster)
    job_resources = sdn_dynamic.resources.get(api_version='v1', kind='Job')
    create_job(batch_v1, job)
    watcher = watch.Watch()

    for event in sdn_dynamic.watch(job_resources, namespace='dnsperf-test', watcher=watcher):
        # print(type(event))
        # print('=====================================================')
        # print(event.keys())
        # pprint(event['raw_object']['status'])
        # print('------------------------------------------------------')
        # pprint('-- v1 job status class --')
        j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items() })
        pprint(j)
        print('======================================================')
        if j.succeeded:
            print('SUCCESS!')
            watcher.stop()
            # delete_job(batch_v1)







    # for event in sdn_ocp_pods.watch(namespace='dnsperf-test'):
    #     # print(str(event['object']))
    #     e = ryaml.loads(str(event['object']))
    #     # print(e)
    #     print(e['ResourceInstance[Pod]']['metadata']['name'])
    #     print(e['ResourceInstance[Pod]']['status']['phase'])
    #     if e['ResourceInstance[Pod]']['status']['phase'] == 'Succeeded':
    #         print(e['ResourceInstance[Pod]']['status']['containerStatuses'][-1]['state']['terminated']['reason'])
    #     print('-----------------------')


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