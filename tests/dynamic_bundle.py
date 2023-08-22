import ray
from ray.util import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

n_node = 1

@ray.remote(num_cpus=1)
def gao():
    print('hahaha')

ray.init('auto')

pg = placement_group(bundles=[{'cpu': 0.5}], strategy='PACK')

ray.wait([pg.ready()], timeout=10)

print('placement group submitted')

t = [
    gao.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=i
        )
    ) for i in range(n_node)
]

pg.add_bundles([{'cpu': 0.5}])

print('bunldes added')

ray.get(t)
