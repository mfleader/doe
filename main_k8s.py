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
from kubernetes.client.models.v1_job_status import V1JobStatus
from kubernetes.client.api.batch_v1_api import BatchV1Api
from kubernetes.client.models.v1_env_var import V1EnvVar

k8s_job_attribute_map = {
    val: key for key, val in V1JobStatus.attribute_map.items()
}

def create_job_object(job_args, es, es_index):
    # Configureate Pod template container
    container = client.V1Container(
        name='container',
        image='quay.io/mleader/dnsdebug:latest',
        image_pull_policy = 'Always',
        command=["/bin/sh", "-c"],
        args=[' '.join(("snafu/run_snafu.py", "-v", "--tool", "dnsperf", *job_args))],
        env=[V1EnvVar(name='es', value=es), V1EnvVar(name='es_index', value=es_index)]
        )
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
        metadata=client.V1ObjectMeta(name=JOB_NAME, namespace='dnsperf-test'),
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


def wait_on_job(trial, api_client, es_url, es_index):
    trial_args = doe.serialize_command_args(trial)
    batch_v1 = BatchV1Api(api_client=api_client)
    job = create_job_object(trial_args, es=es_url, es_index=es_index)
    api_dynamic = DynamicClient(api_client)
    job_resources = api_dynamic.resources.get(api_version='v1', kind='Job')

    print(ryaml.dumps(api_client.sanitize_for_serialization(job)))
    create_job(batch_v1, job)
    watcher = watch.Watch()

    # needs to be async too
    for event in api_dynamic.watch(job_resources, namespace='dnsperf-test', watcher=watcher):
        j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items() })
        print('======================================================')
        if j.succeeded:
            print('SUCCESS!')
            watcher.stop()

    # needs to be async
    delete_job(batch_v1)
    time.sleep(3)


@app.command()
def main(
    experiment_factor_levels_path: str = typer.Argument(...),
    es_url: str = typer.Option(...),
    es_index: str = typer.Option(...),
    sdn_kubeconfig_path: str = typer.Option(...),
    ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(2),
    block_id: int = typer.Option(1)
):
    sdn_cluster = config.new_client_from_config(sdn_kubeconfig_path)
    ovn_cluster = config.new_client_from_config(ovn_kubeconfig_path)

    # myargs = [
    #     "python",
    #     "snafu/run_snafu.py",
    #     "--tool",
    #     "dnsperf",
    #     "-v",
    #     "--run-id",
    #     "88ff59df-117c-46ac-ae4b-ea5962869387",
    #     "--block-id",
    #     "1",
    #     "--load_limit",
    #     "inf",
    #     "--query_path",
    #     "./dnsdebug/noerror.txt",
    #     "--control_plane_nodes",
    #     "3",
    #     "--network_type",
    #     "OpenShiftSDN",
    #     "--transport_mode",
    #     "tcp",
    #     "--client_threads",
    #     "1",
    #     "--trial_id",
    #     "8",
    #     "--runtime-length",
    #     "2"
    # ]

    for trial in doe.main(factor_levels_filepath=experiment_factor_levels_path, block_id=block_id):
        # pprint(trial)

        if trial['network_type'] == 'OpenShiftSDN':
            wait_on_job(trial, sdn_cluster, es_url, es_index)
        elif trial['network_type'] == 'OVNKubernetes':
            wait_on_job(trial, ovn_cluster, es_url, es_index)


    # batch_v1 = BatchV1Api(api_client=sdn_cluster)
    # job = create_job_object(myargs)
    # sdn_dynamic = DynamicClient(sdn_cluster)
    # job_resources = sdn_dynamic.resources.get(api_version='v1', kind='Job')

    # pprint(job)
    # print(ryaml.dumps(sdn_cluster.sanitize_for_serialization(job)))
    # create_job(batch_v1, job)
    # watcher = watch.Watch()

    # for event in sdn_dynamic.watch(job_resources, namespace='dnsperf-test', watcher=watcher):
    #     j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items() })
    #     print('======================================================')
    #     if j.succeeded:
    #         print('SUCCESS!')
    #         watcher.stop()
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



if __name__ == '__main__':
    app()