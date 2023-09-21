import ray
from ray.util import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

@ray.remote(num_cpus=1, num_gpus=1)
def gao(task_id):
    context = ray.get_runtime_context()
    pg_id = context.get_placement_group_id()
    pg_table = ray.util.placement_group_table()
    pg = pg_table[pg_id]
    print(task_id, ray.util.get_current_placement_group().bundle_specs, pg['bundles_to_node_id'], flush=True)

ray.init('auto')

def test_dynamic():
    n_node = 1
    n_task = 2

    pg = placement_group(bundles=[{'CPU': 1, 'GPU': 1}], strategy='STRICT_SPREAD')
    ready, _ = ray.wait([pg.ready()], timeout=5)
    assert ready
    print('placement group submitted')

    t = [
        gao.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i % n_node
            )
        ).remote(i) for i in range(n_task)
    ]

    print('Case no-bundles-added', flush=True)
    ray.get(t)
    
    n_node = 2

    pg.add_bundles([{'CPU': 1, 'GPU': 1}])
    print('Case bunldes-added', flush=True)
    t = [
        gao.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i % n_node
            )
        ).remote(i) for i in range(n_task)
    ]
    ray.get(t)

def test_static():
    pg = placement_group(bundles=[{'CPU': 1}, {'CPU': 1}], strategy='PACK')
    n_node = 2
    n_task = 4
    t = [
        gao.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i % n_node
            )
        ).remote(i) for i in range(n_task)
    ]
    ray.get(t)

if __name__ == '__main__':
    test_dynamic()
    # test_static()