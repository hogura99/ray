import ray
from ray.util import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

n_node = 1

@ray.remote(num_cpus=1)
def gao():
    print(ray.util.get_current_placement_group().bundle_specs)

ray.init('auto')

pg = placement_group(bundles=[{'CPU': 1}], strategy='PACK')

ready, _ = ray.wait([pg.ready()], timeout=5)
assert ready

print('placement group submitted')

t = [
    gao.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=i
        )
    ).remote() for i in range(n_node)
]

print('Case no-bundles-added')

ray.get(t)

pg.add_bundles([{'CPU': 2}])

print('Case bunldes-added')

t = [
    gao.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=i
        )
    ).remote() for i in range(n_node)
]

ray.get(t)