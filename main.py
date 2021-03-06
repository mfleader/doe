import time
from pprint import pprint
import asyncio
from functools import partial
import datetime as dt

import pytz
import typer
from kubernetes import client, config, watch
from kubernetes.dynamic import DynamicClient
from kubernetes.client.models.v1_job_status import V1JobStatus
from kubernetes.client.api.batch_v1_api import BatchV1Api
from kubernetes.client.models.v1_env_var import V1EnvVar
from kubernetes.client.models.v1_volume_mount import V1VolumeMount
import ryaml

import anyio

import doe



app = typer.Typer()
JOB_NAME = "dnsperf-test"
k8s_job_attribute_map = {
    val: key for key, val in V1JobStatus.attribute_map.items()
}


def create_job_object(job_args, es, es_index, cluster_queries):
    # Configureate Pod template container
    container = client.V1Container(
        name='container',
        image='quay.io/mleader/dnsdebug:latest',
        image_pull_policy = 'Always',
        command=["/bin/sh", "-c"],
        args=[' '.join(("python", "snafu/run_snafu.py", "-v", "--tool", "dnsperf", *job_args))],
        env=[V1EnvVar(name='es', value=es), V1EnvVar(name='es_index', value=es_index)],
        volume_mounts=[V1VolumeMount(name='config', mount_path='/opt/dns', read_only=True)]
    )
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        # metadata=client.V1ObjectMeta(labels={"app": JOB_NAME}),
        spec=client.V1PodSpec(
            restart_policy="Never", containers=[container],
            volumes = [
                client.V1Volume(
                    name = 'config',
                    config_map = {
                        'name': 'dnsperf',
                    }
                )
            ]
            )
        )
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
    job.spec.template.spec.containers[0].image = 'quay.io/mleader/dnsdebug:latest'
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


def wait_on_job(trial, api_client, es, es_index, sleep_t, cluster_queries):
    trial_args = doe.serialize_command_args(trial)
    batch_v1 = BatchV1Api(api_client=api_client)
    job = create_job_object(trial_args, es=es, es_index=es_index, cluster_queries=cluster_queries)
    api_dynamic = DynamicClient(api_client)
    job_resources = api_dynamic.resources.get(api_version='v1', kind='Job')
    watcher = watch.Watch()

    print(ryaml.dumps(api_client.sanitize_for_serialization(job)))
    create_job(batch_v1, job)

    # probably should be async
    try:
        for event in api_dynamic.watch(job_resources, namespace='dnsperf-test', watcher=watcher):
            j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items()})
            print('------------------------------------------------------')
            pprint(f'job condition: {j.conditions}')
            if j.succeeded:
                watcher.stop()
    finally:
        # probably should be async
        delete_job(batch_v1)
    time.sleep(sleep_t)


def cluster_queries(api_client):
    dynamic_client = DynamicClient(api_client)
    svc_resources = dynamic_client.resources.get(api_version='v1', kind='Service')
    return '\n'.join(
        (f"{item.metadata.name}.{item.metadata.namespace}.svc.cluster.local A" for item in svc_resources.get().items)
    )


def create_configmap_obj(cluster_queries):
    return client.V1ConfigMap(
        api_version = 'v1',
        kind = "ConfigMap",
        metadata = {
            "name": "dnsperf"
        },
        data = {
            "queries.txt": cluster_queries
            # "queries.txt": "kubernetes.default.svc.cluster.local A"
        },
        # immutable = True
    )


def create_configmap(api_client, configmap):
    dynamic_client = DynamicClient(api_client)
    configmap_api = dynamic_client.resources.get(api_version='v1', kind='ConfigMap')
    res = configmap_api.create(body=configmap, namespace='dnsperf-test')
    print(f'Configmap created {res.status}')


def delete_configmap(api_client):
    dynamic_client = DynamicClient(api_client)
    configmap_api = dynamic_client.resources.get(api_version='v1', kind='ConfigMap')
    res = configmap_api.delete(
        name='dnsperf',
        namespace='dnsperf-test'
    )
    print(f"Configmap {res.status}")


async def _experiment(
    experiment_factor_levels_path: str,
    es: str,
    es_index: str,
    sdn_kubeconfig_path: str,
    # ovn_kubeconfig_path: str,
    sleep_t: int,
    block: int,
    replicate: int,
    measure_repetitions: int
):
    k8s_sdn_api = config.new_client_from_config(sdn_kubeconfig_path)
    # k8s_ovn_api = config.new_client_from_config(ovn_kubeconfig_path)

    # try:
    sdn_queries = cluster_queries(k8s_sdn_api)
    # ovn_queries = cluster_queries(k8s_ovn_api)
    sdn_cm = create_configmap_obj(sdn_queries)
    # ovn_cm = create_configmap_obj(ovn_queries)

    # cleanup old job and config
    # delete_configmap(k8s_sdn_api)
    # delete_configmap(k8s_ovn_api)
    # k8s_sdn_job_api = BatchV1Api(api_client=k8s_sdn_api)
    # delete_job(k8s_sdn_job_api)
    # k8s_ovn_job_api = BatchV1Api(api_client=k8s_ovn_api)
    # delete_job(k8s_ovn_job_api)

    create_configmap(k8s_sdn_api, sdn_cm)
    # create_configmap(k8s_ovn_api, ovn_cm)

    trial_times = []
    completed_trials = 0

    trials = [t for t in doe.main(factor_levels_filepath=experiment_factor_levels_path, block=block)]
    total_trials = len(trials)
    # for input_args in trials:
    #     input_args['repetitions'] = measure_repetitions
    #     input_args['replicate'] = replicate
    #     pprint(input_args)
    #     trial_start = dt.datetime.now()

    #     # if input_args['trial']['network_type'] == 'OpenShiftSDN':
    #     wait_on_job_api = partial(wait_on_job, api_client=k8s_sdn_api, cluster_queries=sdn_queries)
    #     # elif input_args['trial']['network_type'] == 'OVNKubernetes':
    #         # wait_on_job_api = partial(wait_on_job, api_client=k8s_ovn_api, cluster_queries=ovn_queries)

    #     wait_on_job_api(input_args, es=es, es_index=es_index, sleep_t=sleep_t)

    #     trial_end = dt.datetime.now()
    #     completed_trials += 1
    #     trial_times.append((trial_end - trial_start))
    #     trial_time_mean = sum((trial_times), dt.timedelta()) / len(trial_times)
    #     remaining_expected_experiment_time = (total_trials - completed_trials) * trial_time_mean
    #     typer.echo(typer.style(f'Remaining expected experiment time: {remaining_expected_experiment_time}', fg=typer.colors.WHITE, bold=True))
    #     typer.echo(typer.style(f'Expected completion: {dt.datetime.now() + remaining_expected_experiment_time}', fg=typer.colors.BLUE))

    # delete_configmap(k8s_sdn_api)
    # delete_configmap(k8s_ovn_api)


@app.command()
def main(
    experiment_factor_levels_path: str = typer.Argument(...),
    es: str = typer.Option(..., envvar='ELASTICSEARCH_URL'),
    es_index: str = typer.Option('snafu-dnsperf'),
    sdn_kubeconfig_path: str = typer.Option(...),
    # ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(10),
    block: int = typer.Option(1),
    replicate: int = typer.Option(1, help="Experiment run index"),
    measure_repetitions = typer.Option(1)
):
    anyio.run(
        _experiment,
        experiment_factor_levels_path,
        es,
        es_index,
        sdn_kubeconfig_path,
        # ovn_kubeconfig_path,
        sleep_t,
        block,
        replicate,
        measure_repetitions
    )


if __name__ == '__main__':
    app()
