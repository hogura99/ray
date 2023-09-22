import ray
import time
import unittest

from unittest import TestCase
from ray.util import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


N_NODE = 8
N_TASK = 100
GAP = 0.0001


@ray.remote(num_cpus=1, num_gpus=1)
def gao(task_id):
    context = ray.get_runtime_context()
    # pg_id = context.get_placement_group_id()
    # pg_table = ray.util.placement_group_table()
    # time.sleep(GAP)
    # pg = pg_table[pg_id]
    # print(task_id, ray.util.get_current_placement_group().bundle_specs, pg['bundles_to_node_id'], flush=True)
    return context.get_node_id()


class BundleTests(TestCase):

    def setUp(self) -> None:
        ray.init('auto')
        return super().tearDown()

    def tearDown(self) -> None:
        ray.shutdown()
        return super().tearDown()
    
    @staticmethod
    def get_pg(n_node = 1, gpu = 2):
        pg = placement_group(bundles=[
            {'CPU': 1, 'GPU': gpu} for i in range(n_node)
        ], strategy='SPREAD')
        ready, _ = ray.wait([pg.ready()], timeout=5)
        assert ready
        return pg
    
    @staticmethod
    def stats(results):
        cnt = {
            w: 0 for w in results
        }
        for node in results:
            cnt[node] += 1
        return cnt

    def test_add_different_node(self):
        node_add = 2
        pg = self.get_pg(1, 2)
        tasks = []
        for _add in range(node_add + 1):
            n_node = 1 + _add
            t = [
                gao.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=i % n_node
                    )
                ).remote(i) for i in range(N_TASK)
            ]
            time.sleep(GAP * 2)
            pg.add_bundles([{'CPU': 1, 'GPU': 2}])
            tasks.append(t)
            time.sleep(2)
        results = []
        for task in tasks:
            results.append(self.stats(ray.get(task)))
        for i in range(1, len(results)):
            assert len(results[i - 1]) + 1 == len(results[i])
        assert len(results[-1]) == 1 + node_add

    def test_rem_bundle(self):
        n_node = 2
        pg = self.get_pg(n_node, 1)
        tasks = []
        for _del in range(n_node):
            t = [
                gao.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=i % (n_node - _del)
                    )
                ).remote(i) for i in range(N_TASK)
            ]
            time.sleep(GAP * 10)
            if _del != n_node - 1:
                print('remove', _del)
                pg.remove_bundles([n_node - _del - 1])
            tasks.append(t)
            time.sleep(2)
        results = []
        for task in tasks:
            results.append(self.stats(ray.get(task)))
        for i in range(1, len(results)):
            assert len(results[i - 1]) - 1 == len(results[i])
        print(results)
        assert len(results[-1]) == 1

    def test_dynamic_bundle(self):
        pass


def test_dynamic():
    n_node = 1
    n_task = 2

    pg = placement_group(bundles=[{'CPU': 1, 'GPU': 1}], strategy='STRICT_SPREAD')
    ready, _ = ray.wait([pg.ready()], timeout=5)
    assert ready
    print('placement group submitted')

    

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


def suite():
    suite = unittest.TestSuite()
    # suite.addTest(BundleTests("test_add_different_node"))
    suite.addTest(BundleTests("test_rem_bundle"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
    # test_static()