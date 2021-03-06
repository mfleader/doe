import time
from pprint import pprint
import asyncio
from functools import partial
import datetime as dt

import pytz
import typer

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.models.v1_job_status import V1JobStatus
from kubernetes_asyncio.client.api.batch_v1_api import BatchV1Api
from kubernetes_asyncio.client.models.v1_env_var import V1EnvVar

import ryaml

import anyio

import doe



app = typer.Typer()
JOB_NAME = "dnsperf-test-wrapper"
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
        args=[' '.join(("python", "snafu/run_snafu.py", "-v", "--tool", "dnsperf", *job_args))],
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


async def create_job(api_instance, job):
    api_response = await api_instance.create_namespaced_job(
        body=job,
        namespace="dnsperf-test")
    print("Job created. status='%s'" % str(api_response.status))


async def update_job(api_instance, job):
    # Update container image
    job.spec.template.spec.containers[0].image = "perl"
    api_response = await api_instance.patch_namespaced_job(
        name=JOB_NAME,
        namespace="dnsperf-test",
        body=job)
    print("Job updated. status='%s'" % str(api_response.status))


async def delete_job(api_instance):
    api_response = await api_instance.delete_namespaced_job(
        name=JOB_NAME,
        namespace="dnsperf-test",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Job deleted. status='%s'" % str(api_response.status))


async def job_exec(trial, api_client, es, es_index, sleep_t):
    trial_args = doe.serialize_command_args(trial)
    batch_v1 = BatchV1Api(api_client=api_client)
    job = create_job_object(trial_args, es=es, es_index=es_index)
    # job_resources = api_dynamic.resources.get(api_version='v1', kind='Job')
    # watcher = watch.Watch()

    # print(ryaml.dumps(api_client.sanitize_for_serialization(job)))
    await create_job(batch_v1, job)
    await watch_job(api_client)
    await delete_job(batch_v1)
    await anyio.sleep(sleep_t)


async def watch_job(api_client):
    async with watch.Watch().stream(api_client.resources.get(api_version='v1', kind='Job'), namespace='dnsperf-test') as stream:
        async for event in stream:
            j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items()})
            print('------------------------------------------------------')
            pprint(f'job condition: {j.conditions}')


async def _experiment(
    experiment_factor_levels_path: str,
    es: str,
    es_index: str,
    sdn_kubeconfig_path: str,
    ovn_kubeconfig_path: str,
    sleep_t: int,
    block: int,
    replicate: int,
    measure_repetitions: int
):
    k8s_sdn_api = await config.new_client_from_config(sdn_kubeconfig_path)
    k8s_ovn_api = await config.new_client_from_config(ovn_kubeconfig_path)
    k8s_client_apis = {
        'OpenShiftSDN': k8s_sdn_api,
        'OVNKubernetes': k8s_ovn_api
    }
    trial_times = []
    completed_trials = 0

    trials = [t for t in doe.main(factor_levels_filepath=experiment_factor_levels_path, block=block)]
    total_trials = len(trials)
    for input_args in trials:
        input_args['trial']['repetitions'] = measure_repetitions
        input_args['trial']['replicate'] = replicate
        pprint(input_args['trial'])
        trial_start = dt.datetime.now()

        # if input_args['trial']['network_type'] == 'OpenShiftSDN':
        #     wait_on_job_api = partial(wait_on_job, api_client=k8s_sdn_api)
        # elif input_args['trial']['network_type'] == 'OVNKubernetes':
        #     wait_on_job_api = partial(wait_on_job, api_client=k8s_ovn_api)

        await job_exec({**input_args['common'], **input_args['trial']},
            api_client=k8s_client_apis[input_args['trial']['network_type']],
            es=es, es_index=es_index, sleep_t=sleep_t)

        trial_end = dt.datetime.now()
        completed_trials += 1
        trial_times.append((trial_end - trial_start))
        trial_time_mean = sum((trial_times), dt.timedelta()) / len(trial_times)
        remaining_expected_experiment_time = (total_trials - completed_trials) * trial_time_mean
        typer.echo(typer.style(f'Remaining expected experiment time: {remaining_expected_experiment_time}', fg=typer.colors.WHITE, bold=True))
        typer.echo(typer.style(f'Expected completion: {dt.datetime.now() + remaining_expected_experiment_time}', fg=typer.colors.BLUE))


@app.command()
def main(
    experiment_factor_levels_path: str = typer.Argument(...),
    es: str = typer.Option(..., envvar='ELASTICSEARCH_URL'),
    es_index: str = typer.Option('snafu-dnsperf'),
    sdn_kubeconfig_path: str = typer.Option(...),
    ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(120),
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
        ovn_kubeconfig_path,
        sleep_t,
        block,
        replicate,
        measure_repetitions
    )


if __name__ == '__main__':
    app()
