import threading
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import IntegrityError, close_old_connections
from django.test import TransactionTestCase, skipUnlessDBFeature

from wagtail.models import Page, Task, Workflow, WorkflowState, WorkflowTask
from wagtail.signals import workflow_submitted
from wagtail.test.utils import PageFixturesMixin


class TestConcurrentWorkflowStart(PageFixturesMixin, TransactionTestCase):
    fixtures = ["test.json"]

    @skipUnlessDBFeature("supports_transactions", "has_select_for_update")
    def test_concurrent_starts_on_same_object_are_serialized(self):
        page = Page.objects.get(url_path="/home/")
        page.save_revision()
        user = get_user_model().objects.first()
        workflow = Workflow.objects.create(name="concurrent workflow")
        task = Task.objects.create(name="concurrent task")
        WorkflowTask.objects.create(workflow=workflow, task=task, sort_order=1)

        first_submitted = threading.Event()
        release_first = threading.Event()
        second_ready = threading.Event()
        start_second = threading.Event()
        second_validated = threading.Event()
        release_second_validation = threading.Event()
        original_clean = WorkflowState.clean
        results = []

        def on_submitted(sender, instance, user, **kwargs):
            if threading.current_thread().name == "first-start":
                first_submitted.set()
                if not release_first.wait(10):
                    raise TimeoutError("test did not release the first workflow start")

        def instrumented_clean(instance):
            is_second_start = threading.current_thread().name == "second-start"
            try:
                result = original_clean(instance)
            except ValidationError:
                if is_second_start:
                    second_validated.set()
                raise

            if is_second_start:
                second_validated.set()
                if not release_second_validation.wait(10):
                    raise TimeoutError("test did not release the second validation")
            return result

        def start(thread_name):
            close_old_connections()
            try:
                obj = Page.objects.get(pk=page.pk)
                actor = get_user_model().objects.get(pk=user.pk)
                wf = Workflow.objects.get(pk=workflow.pk)
                if thread_name == "second-start":
                    second_ready.set()
                    if not start_second.wait(10):
                        raise TimeoutError("test did not start the second workflow")
                wf.start(obj, actor)
                results.append((thread_name, None))
            except (IntegrityError, TimeoutError, ValidationError) as error:
                results.append((thread_name, error))
            finally:
                close_old_connections()

        workflow_submitted.connect(
            on_submitted, weak=False, dispatch_uid="concurrent-workflow-start-test"
        )
        try:
            with mock.patch.object(WorkflowState, "clean", new=instrumented_clean):
                first = threading.Thread(
                    target=start, args=("first-start",), name="first-start"
                )
                second = threading.Thread(
                    target=start, args=("second-start",), name="second-start"
                )
                first.start()
                self.assertTrue(first_submitted.wait(10))
                second.start()
                self.assertTrue(second_ready.wait(10))
                start_second.set()

                # On the broken implementation, the second transaction passes
                # clean() while the first insert is still uncommitted. With the
                # row lock it waits until the first transaction commits.
                validated_before_first_commit = second_validated.wait(3)
                release_first.set()
                first.join(10)
                self.assertFalse(first.is_alive())

                self.assertTrue(second_validated.wait(10))
                release_second_validation.set()
                second.join(10)
                self.assertFalse(second.is_alive())
        finally:
            release_first.set()
            release_second_validation.set()
            workflow_submitted.disconnect(dispatch_uid="concurrent-workflow-start-test")

        outcomes = dict(results)
        self.assertEqual(len(results), 2, results)
        self.assertIsNone(outcomes["first-start"])
        self.assertFalse(validated_before_first_commit)
        self.assertIsInstance(outcomes["second-start"], ValidationError)
        self.assertEqual(WorkflowState.objects.active().for_instance(page).count(), 1)
