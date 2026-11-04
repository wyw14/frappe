# Copyright (c) 2022, Frappe Technologies and Contributors

# See license.txt

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from rq import exceptions as rq_exc
from rq.job import Job

import frappe
from frappe.core.doctype.rq_job.rq_job import (
	MAX_MATCHED_JOBS,
	RQJob,
	get_wait_time_distribution,
	remove_failed_jobs,
	stop_job,
	validate_wait_boundaries,
	wait_bucket_index,
)
from frappe.installer import update_site_config
from frappe.tests import IntegrationTestCase, timeout
from frappe.tests.utils.test_capabilities import TestService, requires_test_service
from frappe.utils import cstr, execute_in_shell
from frappe.utils.background_jobs import get_job, get_job_status, is_job_enqueued


@timeout(seconds=60)
def wait_for_completion(job: Job):
	while True:
		if not (job.is_queued or job.is_started):
			break
		time.sleep(0.2)


class TestRQJob(IntegrationTestCase):
	BG_JOB = "frappe.core.doctype.rq_job.test_rq_job.test_func"

	def setUp(self) -> None:
		# Cleanup all pending jobs
		for job in frappe.get_all("RQ Job", {"status": "queued"}):
			frappe.get_doc("RQ Job", job.name).cancel()
		return super().setUp()

	def check_status(self, job: Job, status, wait=True):
		if wait:
			wait_for_completion(job)
		self.assertEqual(frappe.get_doc("RQ Job", job.id).status, status)

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_serialization(self):
		job = frappe.enqueue(method=self.BG_JOB, queue="short")
		rq_job = frappe.get_doc("RQ Job", job.id)

		self.assertEqual(job, rq_job.job)
		self.assertDocumentEqual(
			{
				"name": job.id,
				"queue": "short",
				"job_name": self.BG_JOB,
				"exc_info": None,
			},
			rq_job,
		)
		self.check_status(job, "finished")

	def test_configurable_ttl(self):
		frappe.conf.rq_job_failure_ttl = 600
		job = frappe.enqueue(method=self.BG_JOB, queue="short")

		self.assertEqual(job.failure_ttl, 600)

	def test_func_obj_serialization(self):
		job = frappe.enqueue(method=test_func, queue="short")
		rq_job = frappe.get_doc("RQ Job", job.id)
		self.assertEqual(rq_job.job_name, "frappe.core.doctype.rq_job.test_rq_job.test_func")

	def test_renamed_func_obj_serialization(self):
		job = frappe.enqueue(method=test_func, queue="short")
		with patch.dict(globals()):
			del globals()["test_func"]
			rq_job = frappe.get_doc("RQ Job", job.id)
		self.assertEqual(rq_job.job_name, job.description)

	@requires_test_service(TestService.BACKGROUND_WORKER)
	@timeout
	def test_get_list_filtering(self):
		# Check failed job clearning and filtering
		remove_failed_jobs()
		jobs = frappe.get_all("RQ Job", {"status": "failed"})
		self.assertEqual(jobs, [])

		# Pass a job
		job = frappe.enqueue(method=self.BG_JOB, queue="short")
		self.check_status(job, "finished")

		# Fail a job
		job = frappe.enqueue(method=self.BG_JOB, queue="short", fail=True)
		self.check_status(job, "failed")
		jobs = frappe.get_all("RQ Job", {"status": "failed"})
		self.assertEqual(len(jobs), 1)
		self.assertTrue(jobs[0].exc_info)

		# Assert that non-failed job still exists
		non_failed_jobs = frappe.get_all("RQ Job", {"status": ("!=", "failed")})
		self.assertGreaterEqual(len(non_failed_jobs), 1)

		# Create a slow job and check if it's stuck in "Started"
		job = frappe.enqueue(method=self.BG_JOB, queue="short", sleep=10)
		time.sleep(3)
		self.check_status(job, "started", wait=False)
		stop_job(job_id=job.id)
		self.check_status(job, "stopped")

	def test_delete_doc(self):
		job = frappe.enqueue(method=self.BG_JOB, queue="short")
		frappe.get_doc("RQ Job", job.id).delete()

		with self.assertRaises(rq_exc.NoSuchJobError):
			job.refresh()

	def test_queue_filter_rejects_suffix_collision(self):
		"""`queue = long` must not include a custom
		queue whose name is a suffix of `long` (e.g. `schedulelong`).

		The old check `queue.name.endswith(tuple(queues))` false-matched any
		suffix. Fix uses `rsplit(":", 1)[-1] not in queues` so only exact
		short-name matches pass.
		"""
		from unittest.mock import MagicMock, patch

		site = frappe.local.site

		# Two fake queues with bench-prefixed names, one a legitimate `long`
		# and one a custom queue whose short name ends with `long` as a
		# substring.
		long_queue = MagicMock(name="long_queue")
		long_queue.name = "test-bench:long"
		collide_queue = MagicMock(name="collide_queue")
		collide_queue.name = "test-bench:schedulelong"

		def fake_fetch(queue, status):
			# Return one ID per (queue, status), site-prefixed so
			# filter_current_site_jobs doesn't strip them.
			short = queue.name.rsplit(":", 1)[-1]
			return [f"{site}||{short}-{status}"]

		module = "frappe.core.doctype.rq_job.rq_job"
		with (
			patch(f"{module}.get_queues", return_value=[long_queue, collide_queue]),
			patch(f"{module}.get_custom_queues", return_value=["schedulelong"]),
			patch(f"{module}.fetch_job_ids", side_effect=fake_fetch),
		):
			result = RQJob.get_matching_job_ids(filters=[["RQ Job", "queue", "=", "long"]])

		self.assertFalse(
			any("schedulelong" in job_id for job_id in result),
			f"queue=long filter must exclude schedulelong, got: {result!r}",
		)
		self.assertTrue(
			all("||long-" in job_id for job_id in result),
			f"every returned id should be from the long queue, got: {result!r}",
		)
		# One id per status from the single matching queue.
		self.assertEqual(len(result), 7)

	def test_wait_boundaries_validation(self):
		self.assertEqual(validate_wait_boundaries([0, 10, 60]), [0, 10, 60])
		self.assertEqual(validate_wait_boundaries("[0, 10]"), [0, 10])

		invalid = [
			[],
			"[",
			10,
			[-1],
			[1.5],
			[True],
			[1, None],
			[10, 10],
			[10, 5],
			list(range(101)),
		]
		for boundaries in invalid:
			with self.assertRaises(frappe.ValidationError):
				validate_wait_boundaries(boundaries)

	def test_wait_bucket_index_left_closed_right_open(self):
		boundaries = [10, 60, 300]
		self.assertEqual(wait_bucket_index(0, boundaries), 0)
		self.assertEqual(wait_bucket_index(9.999, boundaries), 0)
		self.assertEqual(wait_bucket_index(10, boundaries), 1)
		self.assertEqual(wait_bucket_index(59.999, boundaries), 1)
		self.assertEqual(wait_bucket_index(60, boundaries), 2)
		self.assertEqual(wait_bucket_index(299.999, boundaries), 2)
		self.assertEqual(wait_bucket_index(300, boundaries), 3)
		self.assertEqual(wait_bucket_index(10**9, boundaries), 3)

		# Single boundary: both ends are overflow buckets.
		self.assertEqual(wait_bucket_index(4.99, [5]), 0)
		self.assertEqual(wait_bucket_index(5, [5]), 1)

	def test_get_wait_time_distribution(self):
		module = "frappe.core.doctype.rq_job.rq_job"
		site = frappe.local.site
		now = datetime.now(timezone.utc)

		def aged(seconds):
			job = MagicMock()
			job.enqueued_at = now - timedelta(seconds=seconds)
			return job

		missing = MagicMock()
		missing.enqueued_at = None
		future = MagicMock()
		future.enqueued_at = now + timedelta(minutes=10)

		jobs_by_id = {f"{site}||d{i}": aged(age) for i, age in enumerate([0, 5, 20, 90, 400])}
		jobs_by_id.update(
			{
				f"{site}||l1": aged(15),
				f"{site}||gone": None,  # id still in list, hash already gone
				f"{site}||l2": missing,  # no enqueued_at stored
				f"{site}||l3": future,  # enqueued in the future
				f"{site}||l4": aged(600),
			}
		)

		default_queue = MagicMock()
		default_queue.name = "test-bench:default"
		default_queue.get_job_ids.return_value = [f"{site}||d{i}" for i in range(5)] + [
			f"{site}||d0"  # duplicate listing must count the job once
		]

		long_queue = MagicMock()
		long_queue.name = "test-bench:long"
		long_queue.get_job_ids.return_value = [
			f"{site}||l1",
			"other-site.example||foreign",  # another site must never be fetched or counted
			f"{site}||gone",
			f"{site}||l2",
			f"{site}||l3",
			f"{site}||l4",
		]

		short_queue = MagicMock()
		short_queue.name = "test-bench:short"
		short_queue.get_job_ids.return_value = []

		def fetch_many(job_ids, connection):
			self.assertNotIn("other-site.example||foreign", job_ids)
			return [jobs_by_id[job_id] for job_id in job_ids]

		with (
			patch(f"{module}.get_queues", return_value=[long_queue, short_queue, default_queue]),
			patch(f"{module}.get_redis_conn", return_value=MagicMock()),
			patch(f"{module}.Job.fetch_many", side_effect=fetch_many),
		):
			result = get_wait_time_distribution([10, 60, 300])

		self.assertEqual([q.queue for q in result.queues], ["default", "long", "short"])
		self.assertEqual(
			result.ranges,
			[
				{"lower": None, "upper": 10},
				{"lower": 10, "upper": 60},
				{"lower": 60, "upper": 300},
				{"lower": 300, "upper": None},
			],
		)

		default, long, short = result.queues
		self.assertEqual(default.buckets, [2, 1, 1, 1])
		self.assertEqual(default.observed, 5)
		self.assertEqual(long.buckets, [0, 1, 0, 1])
		self.assertEqual(long.missing_enqueued_at, 1)
		self.assertEqual(long.future_enqueued_at, 1)
		self.assertEqual(long.disappeared, 1)
		self.assertEqual(long.observed, 5)
		self.assertEqual(short.buckets, [0, 0, 0, 0])
		self.assertEqual(short.observed, 0)

		self.assertEqual(result.totals.buckets, [2, 2, 1, 2])
		self.assertEqual(result.totals.missing_enqueued_at, 1)
		self.assertEqual(result.totals.future_enqueued_at, 1)
		self.assertEqual(result.totals.disappeared, 1)
		self.assertEqual(result.totals.observed, 10)

		sampled_at = datetime.fromisoformat(result.sampled_at)
		self.assertEqual(sampled_at.utcoffset(), timedelta(0))
		self.assertIs(result.is_atomic_snapshot, False)
		self.assertTrue(result.note)

		# Only aggregate stats are exposed: never job ids, arguments or traces.
		for queue in result.queues:
			self.assertTrue({"queue", "buckets", "observed"} <= set(queue.keys()))
			self.assertNotIn("job_id", queue)

	def test_get_wait_time_distribution_is_not_truncated_by_list_limit(self):
		module = "frappe.core.doctype.rq_job.rq_job"
		site = frappe.local.site
		count = MAX_MATCHED_JOBS + 100
		now = datetime.now(timezone.utc)

		queue = MagicMock()
		queue.name = "test-bench:default"
		queue.get_job_ids.return_value = [f"{site}||j{i}" for i in range(count)]

		def fetch_many(job_ids, connection):
			return [MagicMock(enqueued_at=now - timedelta(seconds=1)) for _ in job_ids]

		with (
			patch(f"{module}.get_queues", return_value=[queue]),
			patch(f"{module}.get_redis_conn", return_value=MagicMock()),
			patch(f"{module}.Job.fetch_many", side_effect=fetch_many),
		):
			result = get_wait_time_distribution([10, 60])

		self.assertEqual(result.totals.buckets, [count, 0, 0])
		self.assertEqual(result.totals.observed, count)

	def test_get_wait_time_distribution_requires_permission(self):
		with self.set_user("test@example.com"):
			with self.assertRaises(frappe.PermissionError):
				get_wait_time_distribution([10, 60])

	def test_get_wait_time_distribution_live(self):
		job_ids = []
		try:
			for _ in range(2):
				job = frappe.enqueue(method=self.BG_JOB, queue="long")
				job_ids.append(job.id)

			result = get_wait_time_distribution([10, 60, 300])
			queue_names = [q.queue for q in result.queues]
			self.assertEqual(queue_names, sorted(queue_names))
			self.assertIn("long", queue_names)

			long_stats = next(q for q in result.queues if q.queue == "long")
			self.assertEqual(sum(long_stats.buckets), long_stats.observed)
			self.assertGreaterEqual(long_stats.observed, 2)
		finally:
			for job_id in job_ids:
				job = get_job(job_id)
				if job and job.get_status(refresh=False) == "queued":
					job.cancel()

	@requires_test_service(TestService.BACKGROUND_WORKER)
	@timeout
	def test_multi_queue_burst_consumption(self):
		for _ in range(3):
			for q in ["default", "short"]:
				frappe.enqueue(self.BG_JOB, sleep=1, queue=q)

		_, stderr = execute_in_shell("bench worker --queue short,default --burst", check_exit_code=True)
		self.assertIn("quitting", cstr(stderr))

	@requires_test_service(TestService.BACKGROUND_WORKER)
	@timeout
	def test_multi_queue_burst_consumption_worker_pool(self):
		for _ in range(3):
			for q in ["default", "short"]:
				frappe.enqueue(self.BG_JOB, sleep=1, queue=q)

		_, stderr = execute_in_shell(
			"bench worker-pool --queue short,default --burst --num-workers=4",
			check_exit_code=True,
		)
		self.assertIn("quitting", cstr(stderr))

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_job_id_manual_dedup(self):
		job_id = "test_dedup"
		job = frappe.enqueue(self.BG_JOB, sleep=5, job_id=job_id)
		self.assertTrue(is_job_enqueued(job_id))
		self.check_status(job, "finished")
		self.assertFalse(is_job_enqueued(job_id))

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_auto_job_dedup(self):
		job_id = "test_dedup"
		job1 = frappe.enqueue(self.BG_JOB, sleep=2, job_id=job_id, deduplicate=True)
		job2 = frappe.enqueue(self.BG_JOB, sleep=5, job_id=job_id, deduplicate=True)
		self.assertIsNone(job2)
		self.check_status(job1, "finished")  # wait

		# Failed jobs last longer, subsequent job should still pass with same ID.
		job3 = frappe.enqueue(self.BG_JOB, fail=True, job_id=job_id, deduplicate=True)
		self.check_status(job3, "failed")
		job4 = frappe.enqueue(self.BG_JOB, sleep=1, job_id=job_id, deduplicate=True)
		self.check_status(job4, "finished")

	@timeout
	def test_enqueue_after_commit(self):
		job_id = frappe.generate_hash()

		frappe.enqueue(self.BG_JOB, enqueue_after_commit=True, job_id=job_id)
		self.assertIsNone(get_job_status(job_id))

		frappe.db.commit()
		self.assertIsNotNone(get_job_status(job_id))

		job_id = frappe.generate_hash()
		frappe.enqueue(self.BG_JOB, enqueue_after_commit=True, job_id=job_id)
		self.assertIsNone(get_job_status(job_id))

		frappe.db.rollback()
		self.assertIsNone(get_job_status(job_id))

		frappe.db.commit()
		self.assertIsNone(get_job_status(job_id))

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_memory_usage(self):
		if frappe.db.db_type != "mariadb":
			return
		job = frappe.enqueue("frappe.utils.data._get_rss_memory_usage")
		self.check_status(job, "finished")

		rss = job.latest_result().return_value
		msg = """Memory usage of simple background job increased. Potential root cause can be a newly added python module import. Check and move them to approriate file/function to avoid loading the module by default."""

		# If this starts failing analyze memory usage using memray or some equivalent tool to find
		# offending imports/function calls.
		# Refer this PR: https://github.com/frappe/frappe/pull/21467
		LAST_MEASURED_USAGE = 46
		if frappe.conf.use_mysqlclient:
			# TEMP: Add extra allowance for running two connectors, this should be rolled back before v16
			LAST_MEASURED_USAGE += 2

		# Observed higher usage on 3.14. Temporarily raising the limit
		LAST_MEASURED_USAGE += 6

		# TODO: Observed higher usage on 2026-05-26. Temporarily raising the limit
		LAST_MEASURED_USAGE += 1

		# Setting session time zone on connect loads System Settings in the worker
		LAST_MEASURED_USAGE += 3

		self.assertLessEqual(rss, LAST_MEASURED_USAGE * 1.05, msg)

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_clear_failed_jobs(self):
		limit = 10
		update_site_config("rq_failed_jobs_limit", limit)

		jobs = [frappe.enqueue(method=self.BG_JOB, queue="short", fail=True) for _ in range(limit * 2)]
		self.check_status(jobs[-1], "failed")
		# Count only the queue this test enqueues to. truncate_failed_registry trims each queue to
		# the limit, but get_count sums failed jobs across *all* queues -- failed jobs left by
		# sibling tests in other queues would otherwise push the count over the limit (seen as
		# "13 not less than or equal to 12.0", intermittently and especially on postgres).
		self.assertLessEqual(
			RQJob.get_count(filters=[["RQ Job", "status", "=", "failed"], ["RQ Job", "queue", "=", "short"]]),
			limit * 1.2,
		)

	@requires_test_service(TestService.BACKGROUND_WORKER)
	def test_pickle_lazy_doc_for_rq_job(self):
		job = frappe.enqueue(test_serialization, user=frappe.get_lazy_doc("User", "Guest"))
		self.check_status(job, "finished")


def test_serialization(user):
	assert user.roles
	return True


def test_func(fail=False, sleep=0):
	if fail:
		42 / 0
	if sleep:
		time.sleep(sleep)

	return True
