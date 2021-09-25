import time
from pprint import pprint
import asyncio
from functools import partial
import datetime as dt

import typer
from kubernetes import client, config, watch
from kubernetes.dynamic import DynamicClient
from kubernetes.client.models.v1_job_status import V1JobStatus
from kubernetes.client.api.batch_v1_api import BatchV1Api
from kubernetes.client.models.v1_env_var import V1EnvVar
import ryaml
import pandas as pd

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


def wait_on_job(trial, api_client, es, es_index, sleep_t):
    trial_args = doe.serialize_command_args(trial)
    batch_v1 = BatchV1Api(api_client=api_client)
    job = create_job_object(trial_args, es=es, es_index=es_index)
    api_dynamic = DynamicClient(api_client)
    job_resources = api_dynamic.resources.get(api_version='v1', kind='Job')
    watcher = watch.Watch()

    # print(ryaml.dumps(api_client.sanitize_for_serialization(job)))
    create_job(batch_v1, job)

    # probably should be async
    for event in api_dynamic.watch(job_resources, namespace='dnsperf-test', watcher=watcher):
        j = V1JobStatus(**{k8s_job_attribute_map[key]: val for key,val in event['raw_object']['status'].items()})
        print('------------------------------------------------------')
        pprint(f'job condition: {j.conditions}')
        if j.succeeded:
            watcher.stop()

    # probably should be async
    delete_job(batch_v1)
    time.sleep(sleep_t)


@app.command()
def main(
    experiment_factor_levels_path: str = typer.Argument(...),
    es: str = typer.Option(..., envvar='ELASTICSEARCH_URL'),
    es_index: str = typer.Option('snafu-dnsperf'),
    sdn_kubeconfig_path: str = typer.Option(...),
    ovn_kubeconfig_path: str = typer.Option(...),
    sleep_t: int = typer.Option(120),
    block_init_id: int = typer.Option(1),
    replicates: int = typer.Option(1)
):
    sdn_cluster = config.new_client_from_config(sdn_kubeconfig_path)
    ovn_cluster = config.new_client_from_config(ovn_kubeconfig_path)
    trial_times = []
    completed_trials = 0

    for i in range(block_init_id, block_init_id + replicates):
        trials = [t for t in doe.main(factor_levels_filepath=experiment_factor_levels_path, block=i)]
        total_trials = replicates * len(trials)
        for trial in trials:
            pprint(trial)
            trial_start = dt.datetime.now()

            if trial['network_type'] == 'OpenShiftSDN':
                wait_on_job_api = partial(wait_on_job, api_client=sdn_cluster)
            elif trial['network_type'] == 'OVNKubernetes':
                wait_on_job_api = partial(wait_on_job, api_client=ovn_cluster)
            wait_on_job_api(trial, es=es, es_index=es_index, sleep_t=sleep_t)

            trial_end = dt.datetime.now()
            completed_trials += 1
            trial['time_len'] = trial_end - trial_start
            trial_times.append(trial)
            trial_time_mean = sum(map(lambda x: x['time_len'], trial_times), dt.timedelta()) / len(trial_times)
            remaining_expected_experiment_time = (total_trials - completed_trials) * trial_time_mean
            typer.echo(typer.style(f'Remaining expected experiment time: {remaining_expected_experiment_time}', fg=typer.colors.WHITE, bold=True))
            typer.echo(typer.style(f'Expected completion: {dt.datetime.now().astimezone() + remaining_expected_experiment_time}', fg=typer.colors.BLUE))

    trial_df = pd.DataFrame.from_records(trials)
    trial_df.to_csv(f"dnsperf_trial_times-{dt.isoformat(dt.datetime.now().astimezone()).csv}",index=False)


if __name__ == '__main__':
    app()