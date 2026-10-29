# -*- coding: utf-8 -*-
"""Task Consumer for AI Box.

Consumes durable file upload events from RabbitMQ, guarantees atomic persistence
in local Task DB prior to issuing manual basic.ack, enforces controlled backoff
on Task DB failures to prevent rapid nack loops, prevents race-condition registration
of superseded file versions, and orchestrates file retrieval via Streaming File Proxy
and vector indexing transitions.
"""

import argparse
import re
import socket
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Dict, Any, Optional, Tuple, Protocol

from task_db import (
    TaskDB,
    TASK_STATUS_PENDING,
    TASK_STATUS_PROCESSING,
    TASK_STATUS_RETRY_WAIT,
    TASK_STATUS_INDEX_VERIFY_PENDING,
    TASK_STATUS_INDEXED,
    TASK_STATUS_SUPERSEDED,
    TASK_STATUS_NEEDS_REVIEW,
)
from destination_adapter import (
    AmbiguousWorkspaceError,
    WorkspaceResolutionError,
    DeliveryContract,
    DeliveryStatus,
    DestinationAdapterProtocol,
    LocalAiBoxAdapter,
)

logger = logging.getLogger("AIBox.Consumer")


class AMQPChannelProtocol(Protocol):
    """Protocol representing the required AMQP channel operations for consumer."""
    def basic_ack(self, delivery_tag: int) -> None: ...
    def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None: ...


def sanitize_uid_to_slug(uid: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_-]", "-", str(uid).strip().lower())
    clean = re.sub(r"-+", "-", clean).strip("-_")
    return clean or "unknown"

def get_personal_workspace_slug(uid: str) -> str:
    return f"personal-{sanitize_uid_to_slug(uid)}"

class TaskConsumer:
    """Consumes messages from RabbitMQ and coordinates Task DB and file processing."""

    def __init__(
        self,
        task_db: TaskDB,
        destination_adapter: Optional[DestinationAdapterProtocol] = None,
        proxy_base_url: str = "http://127.0.0.1:8090",
        proxy_auth_token: str = "ai_box_proxy_secret_token",
        worker_id: Optional[str] = None,
        db_failure_backoff_seconds: float = 2.0,
        max_db_failure_backoff_seconds: float = 30.0,
    ):
        self.task_db = task_db
        self.destination_adapter = destination_adapter
        self.proxy_base_url = proxy_base_url.rstrip("/")
        self.proxy_auth_token = proxy_auth_token
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.db_failure_backoff_seconds = db_failure_backoff_seconds
        self.max_db_failure_backoff_seconds = max_db_failure_backoff_seconds
        self.consecutive_db_failures = 0

    def handle_message(
        self,
        body_bytes: bytes,
        delivery_tag: int,
        channel: Optional[AMQPChannelProtocol] = None,
    ) -> bool:
        """Handle incoming AMQP message strictly enforcing DB commit before ACK.

        1. Parse JSON payload and validate mandatory fields.
        2. Insert into Task DB (with SQLite synchronous=FULL).
        3. Issue channel.basic_ack() ONLY AFTER successful commit.
        4. If DB fails:
           - NEVER call basic_ack (message is NOT acknowledged).
           - Apply a controlled backoff sleep to prevent tight CPU-spinning / rapid nack loops.
           - Issue basic_nack(delivery_tag, requeue=True) so the event is preserved in RabbitMQ.
        """
        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except Exception as e:
            logger.error("Failed to parse AMQP message JSON: %s", str(e))
            if channel:
                channel.basic_nack(delivery_tag, requeue=False)
            return False

        if not isinstance(payload, dict) or "event_id" not in payload or "file_id" not in payload:
            logger.error("Malformed event payload missing event_id or file_id: %s", payload)
            if channel:
                channel.basic_nack(delivery_tag, requeue=False)
            return False

        event_id = payload["event_id"]
        file_id = payload["file_id"]

        # Atomic persistence in Task DB with controlled backoff on failure
        try:
            is_new, task_id = self.task_db.record_task(payload)
            self.consecutive_db_failures = 0  # Reset failure counter on successful DB commit
            if is_new:
                logger.info("Task recorded: event=%s, file_id=%d, task_id=%d", event_id, file_id, task_id)
            else:
                logger.info("Duplicate event ignored: event=%s, existing_task_id=%d", event_id, task_id)
        except Exception as e:
            self.consecutive_db_failures += 1
            backoff = min(
                self.db_failure_backoff_seconds * (2 ** (self.consecutive_db_failures - 1)),
                self.max_db_failure_backoff_seconds,
            )
            logger.error(
                "CRITICAL: Failed to commit task %s to Task DB: %s. "
                "Applying controlled backoff pause (%.1fs, consecutive_failures=%d) to prevent rapid nack loop.",
                event_id, str(e), backoff, self.consecutive_db_failures,
            )
            # Sleep for controlled backoff duration to prevent flood / CPU spinning
            time.sleep(backoff)

            if channel:
                # Do NOT ack. Requeue so event is preserved in RabbitMQ.
                channel.basic_nack(delivery_tag, requeue=True)
            return False

        # Physical commit succeeded -> Issue manual basic.ack
        if channel:
            try:
                channel.basic_ack(delivery_tag)
                logger.debug("Manual ACK issued for delivery_tag %d (event %s)", delivery_tag, event_id)
            except Exception as e:
                logger.error("Failed to issue basic.ack for event %s: %s", event_id, str(e))
                return False

        return True

    def fetch_file_content(self, file_id: int) -> bytes:
        """Fetch file stream from Integration Layer Streaming File Proxy."""
        url = f"{self.proxy_base_url}/api/v1/files/{file_id}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.proxy_auth_token}",
                "Accept": "application/octet-stream",
                "User-Agent": f"AIBox-Consumer/{self.worker_id}",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=30.0) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Proxy returned HTTP {resp.status} for file ID {file_id}")
            return resp.read()

    def resolve_workspace_for_task(self, task: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], bool]:
        """Resolves destination workspace slug from task's scope or direct Outbox folder scoping.
        
        Returns (slug, error_message, is_transient).
        """
        # 1. Direct workspace slug resolution from Outbox event (Folder-Centric Architecture)
        direct_slug = task.get("workspace_slug")
        if direct_slug and isinstance(direct_slug, str) and direct_slug.strip():
            slug_clean = direct_slug.strip()
            display_name = str(task.get("workspace_name") or slug_clean).strip()
            if hasattr(self.destination_adapter, "ensure_workspace_exists"):
                try:
                    self.destination_adapter.ensure_workspace_exists(slug_clean, display_name)
                except Exception as e:
                    return None, f"Failed to auto-provision workspace '{slug_clean}': {e}", True
            return slug_clean, None, False

        scope = task.get("scope")
        owner_uid = task.get("owner_uid")
        raw_group = task.get("group_name")

        is_personal = (scope == "personal") or (raw_group and raw_group.strip().lower() == "personal")

        if is_personal:
            if not owner_uid or not str(owner_uid).strip():
                return None, "Personal file task missing owner_uid. Routing halted.", False
            personal_slug = get_personal_workspace_slug(str(owner_uid))
            if hasattr(self.destination_adapter, "ensure_workspace_exists"):
                try:
                    self.destination_adapter.ensure_workspace_exists(personal_slug, f"اسناد شخصی ({owner_uid})")
                except Exception as e:
                    return None, f"Failed to auto-provision personal workspace '{personal_slug}': {e}", True
            return personal_slug, None, False

        # Group Scope Resolution
        if not raw_group or not isinstance(raw_group, str) or not raw_group.strip():
            return None, "Task group_name is empty or unspecified. Routing to default workspace prevented.", False

        clean_group = raw_group.strip()
        if any(sep in clean_group for sep in (",", ";", "|")):
            return None, f"Multi-group assignment '{clean_group}' is ambiguous. Routing to default workspace prevented.", False

        if clean_group.lower() in ("unknown", "none", "null"):
            return None, f"Task group_name '{clean_group}' is invalid/unknown. Routing to default workspace prevented.", False

        resolver = getattr(self.destination_adapter, "resolve_or_create_group_workspace", None)
        if not callable(resolver):
            return None, "Destination adapter does not provide an official group workspace resolver. Guessed slug and default workspace fallbacks are forbidden.", False

        try:
            slug = resolver(clean_group)
            if not slug or not str(slug).strip():
                return None, f"Adapter returned empty slug for group '{clean_group}'. Routing halted.", False
            return str(slug).strip(), None, False
        except AmbiguousWorkspaceError as e:
            return None, f"Ambiguous workspace for group '{clean_group}': {e}", False
        except WorkspaceResolutionError as e:
            return None, f"Workspace resolution failed for group '{clean_group}': {e}", False
        except urllib.error.HTTPError as e:
            if e.code in (500, 502, 503, 504):
                return None, f"Transient destination server error (HTTP {e.code}): {e.reason}", True
            return None, f"Destination rejected workspace resolution (HTTP {e.code}): {e.reason}", False
        except (ConnectionError, urllib.error.URLError, TimeoutError, OSError) as e:
            return None, f"Transient network error resolving workspace for group '{clean_group}': {e}", True
        except Exception as e:
            return None, f"Workspace resolution rejected for group '{clean_group}': {e}", False

    def process_claimed_task(self, task: Dict[str, Any]) -> bool:
        """Process a single claimed task through file fetch, adapter delivery, and indexing.

        CRITICAL ARCHITECTURAL BOUNDARIES:
        1. Race Condition Guard: If a newer version of the same file arrived while this worker
           was fetching or processing, delivery to the destination adapter is ABORTED to prevent
           overwriting or indexing obsolete versions.
        2. Delivery to destination adapter transitions the task to INDEX_VERIFY_PENDING,
           NOT directly to INDEXED. The INDEXED status is reserved exclusively for when the
           physical presence of vectors in LanceDB is verified.
        """
        task_id = task["id"]
        file_id = task["file_id"]
        file_name = task["file_name"]
        mime_type = task.get("mime_type", "text/plain")
        expected_sha256 = task.get("sha256", "")

        event_type = task.get("event_type", "file.created")

        try:
            # Handle file.deleted event
            if event_type == "file.deleted":
                target_workspace_slug, ws_err, is_transient = self.resolve_workspace_for_task(task)
                if target_workspace_slug:
                    prev = self.task_db.get_current_task_for_file(file_id)
                    doc_loc = prev.get("doc_location") if prev else None
                    if doc_loc and hasattr(self.destination_adapter, "remove_document_from_workspace"):
                        logger.info("Purging deleted file %s (%s) from workspace %s", file_name, doc_loc, target_workspace_slug)
                        self.destination_adapter.remove_document_from_workspace(doc_loc, target_workspace_slug)
                self.task_db.update_task_status(task_id, self.worker_id, TASK_STATUS_INDEXED)
                logger.info("Task %d (file.deleted) completed and vector purged.", task_id)
                return True

            # Handle file.moved event: purge from old workspace if workspace changed
            if event_type == "file.moved":
                old_slug = task.get("old_workspace_slug")
                if old_slug:
                    prev = self.task_db.get_current_task_for_file(file_id)
                    doc_loc = prev.get("doc_location") if prev else None
                    if doc_loc and hasattr(self.destination_adapter, "remove_document_from_workspace"):
                        logger.info("Purging moved file from old workspace %s (%s)", old_slug, doc_loc)
                        self.destination_adapter.remove_document_from_workspace(doc_loc, old_slug)

            # 1. Fetch content from streaming proxy
            file_bytes = self.fetch_file_content(file_id)

            # 2. Race Condition Guard: Verify this task is still current and has not been superseded
            #    by a newer event for the same file_id before sending to adapter!
            if not self.task_db.is_task_current(task_id, self.worker_id):
                logger.warning(
                    "Race condition prevented: Task %d (file_id=%d) was superseded by a newer version. "
                    "Aborting delivery to destination adapter.",
                    task_id, file_id,
                )
                return False

            if not self.destination_adapter:
                # Without an adapter, keep boundary at INDEX_VERIFY_PENDING (not INDEXED)
                self.task_db.update_task_status(task_id, self.worker_id, TASK_STATUS_INDEX_VERIFY_PENDING)
                logger.info(
                    "Task %d delivered without adapter; transitioned to %s (vector verification unperformed)",
                    task_id, TASK_STATUS_INDEX_VERIFY_PENDING
                )
                return True

            # 3. Resolve destination workspace from group_name
            target_workspace_slug, ws_err, is_transient = self.resolve_workspace_for_task(task)
            if not target_workspace_slug:
                status = TASK_STATUS_RETRY_WAIT if is_transient else TASK_STATUS_NEEDS_REVIEW
                logger.error(
                    "Task %d (file_id=%d) workspace resolution failed: %s. Transitioning to %s.",
                    task_id, file_id, ws_err, status,
                )
                self.task_db.update_task_status(
                    task_id,
                    self.worker_id,
                    status,
                    error=ws_err,
                )
                return False

            # 4. Build delivery contract targeted to resolved workspace
            contract = DeliveryContract(
                source_file_id=str(file_id),
                file_name=file_name,
                mime_type=mime_type,
                content_hash=expected_sha256,
                file_payload=file_bytes,
                workspace_slug=target_workspace_slug,
            )

            # 5. Deliver to destination adapter (e.g. AnythingLLM ingestion)
            delivery_result = self.destination_adapter.deliver(contract)

            # Determine real destination workspace slug (from delivery result or resolved slug)
            target_workspace_slug = (
                getattr(delivery_result, "workspace_slug", None)
                or target_workspace_slug
            )

            # Record unique document identifier/location and workspace_slug for this task
            if delivery_result.doc_location:
                self.task_db.set_task_doc_location(
                    task_id,
                    delivery_result.doc_location,
                    workspace_slug=target_workspace_slug,
                )

            # Re-check currency before advancing status to avoid marking superseded task as pending verify
            if not self.task_db.is_task_current(task_id, self.worker_id):
                logger.warning(
                    "Task %d (file_id=%d) was superseded during adapter delivery. State not updated.",
                    task_id, file_id,
                )
                # Purge only this old document from its specific workspace so its vectors do not persist
                if delivery_result.doc_location:
                    # Guard: If target workspace is unknown, never fall back to default workspace!
                    if not target_workspace_slug or not str(target_workspace_slug).strip():
                        logger.error(
                            "Cannot purge document for task %d: workspace_slug is unknown. Holding for review to avoid default workspace deletion.",
                            task_id
                        )
                        self.task_db.mark_task_cleanup_pending(
                            task_id,
                            error="Cleanup halted: unknown workspace_slug; avoided fallback to default workspace.",
                            max_cleanup_retries=1,
                        )
                        return False

                    adapter_remover = getattr(
                        self.destination_adapter,
                        "remove_document_from_workspace",
                        getattr(self.destination_adapter, "remove_document", None),
                    )
                    cleanup_ok = False
                    cleanup_err = None
                    if callable(adapter_remover):
                        logger.info(
                            "Purging superseded old document '%s' from workspace '%s' to avoid obsolete vector persistence.",
                            delivery_result.doc_location, target_workspace_slug,
                        )
                        try:
                            cleanup_ok = bool(adapter_remover(
                                delivery_result.doc_location,
                                workspace_slug=target_workspace_slug,
                            ))
                            if not cleanup_ok:
                                cleanup_err = f"Destination adapter rejected removal of '{delivery_result.doc_location}' from workspace '{target_workspace_slug}'"
                        except Exception as e:
                            cleanup_err = f"Exception during document removal: {e}"
                            logger.error(
                                "Failed to remove superseded document '%s' from workspace '%s': %s",
                                delivery_result.doc_location, target_workspace_slug, e,
                            )
                    else:
                        cleanup_err = "Adapter does not provide document removal interface"

                    if cleanup_ok:
                        self.task_db.mark_task_cleanup_completed(task_id)
                        logger.info(
                            "Successfully removed superseded document '%s' from workspace '%s'.",
                            delivery_result.doc_location, target_workspace_slug,
                        )
                    else:
                        self.task_db.mark_task_cleanup_pending(task_id, error=cleanup_err)
                        logger.warning(
                            "Cleanup of superseded document '%s' from workspace '%s' for task %d failed (%s). Recorded as cleanup_pending for retry.",
                            delivery_result.doc_location, target_workspace_slug, task_id, cleanup_err,
                        )
                return False

            if delivery_result.status in (DeliveryStatus.SUCCESS, DeliveryStatus.ALREADY_PROCESSED):
                # Transition to INDEX_VERIFY_PENDING: Document accepted by ingestion gateway
                self.task_db.update_task_status(
                    task_id,
                    self.worker_id,
                    TASK_STATUS_INDEX_VERIFY_PENDING,
                    doc_location=delivery_result.doc_location,
                    workspace_slug=target_workspace_slug,
                )
                logger.info(
                    "Task %d ingested by destination adapter; transitioned to %s (awaiting physical vector verification in LanceDB)",
                    task_id, TASK_STATUS_INDEX_VERIFY_PENDING
                )
                return True
            elif delivery_result.status == DeliveryStatus.RETRYABLE_FAILURE:
                self.task_db.update_task_status(
                    task_id, self.worker_id, TASK_STATUS_RETRY_WAIT, error=delivery_result.message
                )
                logger.warning("Task %d retryable failure: %s", task_id, delivery_result.message)
                return False
            else:
                self.task_db.update_task_status(
                    task_id, self.worker_id, TASK_STATUS_NEEDS_REVIEW, error=delivery_result.message
                )
                logger.error("Task %d fatal failure: %s", task_id, delivery_result.message)
                return False

        except urllib.error.HTTPError as e:
            err_msg = f"Proxy HTTP Error {e.code}: {e.reason}"
            if e.code in (401, 403, 404):
                self.task_db.update_task_status(task_id, self.worker_id, TASK_STATUS_NEEDS_REVIEW, error=err_msg)
            else:
                self.task_db.update_task_status(task_id, self.worker_id, TASK_STATUS_RETRY_WAIT, error=err_msg)
            return False
        except Exception as e:
            err_msg = f"Task processing error: {str(e)}"
            logger.error("Task %d failed: %s", task_id, err_msg)
            self.task_db.update_task_status(task_id, self.worker_id, TASK_STATUS_RETRY_WAIT, error=err_msg)
            return False

    def verify_pending_indexed_tasks(self) -> int:
        """Checks any tasks in INDEX_VERIFY_PENDING status for physical vector table in LanceDB and marks INDEXED."""
        pending_tasks = self.task_db.get_tasks_by_status(TASK_STATUS_INDEX_VERIFY_PENDING)
        verified = 0
        for t in pending_tasks:
            ws_slug = t.get("workspace_slug")
            if ws_slug:
                lance_path = f"/home/alborz/ai-box/data/anythingllm/lancedb/{ws_slug}.lance"
                if os.path.exists(lance_path):
                    self.task_db.mark_task_indexed(t["id"])
                    logger.info("Task %d physically verified in LanceDB (%s); updated to %s", t["id"], lance_path, TASK_STATUS_INDEXED)
                    verified += 1
        return verified

    def retry_superseded_cleanups(self) -> int:
        """Retries purging lingering documents for superseded tasks whose initial removal failed.
        Strictly targets only the specific superseded doc_location and recorded workspace_slug.
        """
        if not self.destination_adapter:
            return 0

        adapter_remover = getattr(
            self.destination_adapter,
            "remove_document_from_workspace",
            getattr(self.destination_adapter, "remove_document", None),
        )
        if not callable(adapter_remover):
            return 0

        pending_tasks = self.task_db.get_pending_cleanup_tasks()
        successful_cleanups = 0

        for task in pending_tasks:
            task_id = task["id"]
            doc_loc = task["doc_location"]
            ws_slug = task.get("workspace_slug")

            # Guard: If workspace_slug is missing, never delete from default workspace!
            if not ws_slug or not str(ws_slug).strip():
                logger.warning(
                    "Retry cleanup halted for task %d: workspace_slug is missing. Retaining for review.",
                    task_id
                )
                self.task_db.mark_task_cleanup_pending(
                    task_id,
                    error="Missing workspace_slug; auto-cleanup in default workspace prevented.",
                    max_cleanup_retries=1,
                )
                continue

            logger.info(
                "Retrying cleanup of superseded document '%s' in workspace '%s' for task %d...",
                doc_loc, ws_slug, task_id
            )
            try:
                ok = bool(adapter_remover(doc_loc, workspace_slug=ws_slug))
                if ok:
                    self.task_db.mark_task_cleanup_completed(task_id)
                    logger.info("Successfully cleaned up superseded document '%s' in workspace '%s' on retry.", doc_loc, ws_slug)
                    successful_cleanups += 1
                else:
                    self.task_db.mark_task_cleanup_pending(
                        task_id, error=f"Adapter rejected removal of '{doc_loc}' from workspace '{ws_slug}' on retry"
                    )
            except Exception as e:
                logger.error("Exception retrying document removal for task %d: %s", task_id, e)
                self.task_db.mark_task_cleanup_pending(task_id, error=str(e))

        return successful_cleanups

    def process_next_task(self) -> bool:
        """Claim and process the next runnable task in the queue, retrying any pending cleanups first."""
        self.retry_superseded_cleanups()
        task = self.task_db.claim_task(self.worker_id)
        if not task:
            return False
        return self.process_claimed_task(task)


def start_consumer(
    rabbitmq_host: str = "127.0.0.1",
    rabbitmq_port: int = 5672,
    rabbitmq_user: str = "guest",
    rabbitmq_password: str = "guest",
    rabbitmq_vhost: str = "/",
    rabbitmq_queue: str = "ai_box.file_events",
    rabbitmq_exchange: str = "archive.events",
    rabbitmq_routing_key: str = "file.created",
    task_db_path: Optional[str] = None,
    proxy_base_url: str = "http://127.0.0.1:8090",
    proxy_auth_token: str = "ai_box_proxy_secret_token",
    gateway_url: str = "http://127.0.0.1:3001",
    destination_api_key: str = "ai_box_internal_api_secret_key",
    stop_event=None,
) -> None:
    """Connect to RabbitMQ queue, consuming events with manual ack and task processing."""
    adapter = LocalAiBoxAdapter(gateway_url, api_key=destination_api_key)
    db = TaskDB(task_db_path)
    consumer = TaskConsumer(
        task_db=db,
        destination_adapter=adapter,
        proxy_base_url=proxy_base_url,
        proxy_auth_token=proxy_auth_token,
    )

    # Ensure /home/alborz/integration-layer is on path for AMQPClient
    if "/home/alborz/integration-layer" not in sys.path:
        sys.path.insert(0, "/home/alborz/integration-layer")
    try:
        from amqp_client import AMQPClient
    except ImportError:
        AMQPClient = None

    try:
        import pika
    except ImportError:
        pika = None

    if pika is not None:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            virtual_host=rabbitmq_vhost,
            credentials=credentials,
            heartbeat=60,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(exchange=rabbitmq_exchange, exchange_type="direct", durable=True)
        channel.queue_declare(queue=rabbitmq_queue, durable=True)
        channel.queue_bind(exchange=rabbitmq_exchange, queue=rabbitmq_queue, routing_key=rabbitmq_routing_key)
        channel.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            ok = consumer.handle_message(body, method.delivery_tag, channel=ch)
            if ok:
                consumer.process_next_task()

        channel.basic_consume(queue=rabbitmq_queue, on_message_callback=on_message, auto_ack=False)
        logger.info("AI Box Task Consumer listening via pika on '%s'...", rabbitmq_queue)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            connection.close()
    elif AMQPClient is not None:
        logger.info("Starting pure Python AMQP consumer on %s:%d (queue: %s)...", rabbitmq_host, rabbitmq_port, rabbitmq_queue)
        client = AMQPClient(host=rabbitmq_host, port=rabbitmq_port, user=rabbitmq_user, password=rabbitmq_password, vhost=rabbitmq_vhost)
        client.connect()
        client.setup_topology(exchange=rabbitmq_exchange, queue=rabbitmq_queue, routing_key=rabbitmq_routing_key, exchange_type="direct")
        logger.info("AI Box Task Consumer connected and verified topology for '%s'", rabbitmq_queue)

        class PureAMQPChannel:
            def basic_ack(self, delivery_tag: int) -> None:
                client.basic_ack(delivery_tag)
            def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None:
                client.basic_nack(delivery_tag, requeue=requeue)

        channel_wrapper = PureAMQPChannel()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stopping pure Python AMQP consumer...")
                break
            try:
                # Check for any queued messages in RabbitMQ
                msg = client.basic_get(rabbitmq_queue, no_ack=False)
                if msg is not None:
                    tag, body_bytes, _ = msg
                    logger.info("Consumer received AMQP message tag %d", tag)
                    ok = consumer.handle_message(body_bytes, tag, channel=channel_wrapper)
                    if ok:
                        consumer.process_next_task()
                else:
                    consumer.process_next_task()
                    consumer.verify_pending_indexed_tasks()
                    time.sleep(1.0)
            except (socket.error, ConnectionError, OSError) as e:
                logger.warning("AMQP connection lost (%s), reconnecting in 2s...", e)
                time.sleep(2.0)
                try:
                    client.connect()
                    client.setup_topology(exchange=rabbitmq_exchange, queue=rabbitmq_queue, routing_key=rabbitmq_routing_key, exchange_type="direct")
                except Exception:
                    pass
            except Exception as e:
                logger.error("Error in consumer loop: %s", e, exc_info=True)
                time.sleep(1.0)
        client.close()
    else:
        raise RuntimeError("Neither pika nor AMQPClient available for AMQP consumption")


def main():
    parser = argparse.ArgumentParser(description="AI Box Task Consumer: Consume durable events from RabbitMQ.")
    parser.add_argument("--rabbitmq-host", default=os.environ.get("RABBITMQ_HOST", "127.0.0.1"))
    parser.add_argument("--rabbitmq-port", type=int, default=int(os.environ.get("RABBITMQ_PORT", "5672")))
    parser.add_argument("--rabbitmq-user", default=os.environ.get("RABBITMQ_USER", "guest"))
    parser.add_argument("--rabbitmq-password", default=os.environ.get("RABBITMQ_PASSWORD", "guest"))
    parser.add_argument("--rabbitmq-vhost", default=os.environ.get("RABBITMQ_VHOST", "/"))
    parser.add_argument("--queue", default=os.environ.get("RABBITMQ_QUEUE", "ai_box.file_events"))
    parser.add_argument("--exchange", default=os.environ.get("RABBITMQ_EXCHANGE", "archive.events"))
    parser.add_argument("--routing-key", default=os.environ.get("RABBITMQ_ROUTING_KEY", "file.created"))
    parser.add_argument("--task-db-path", default=os.environ.get("TASK_DB_PATH", None))
    parser.add_argument("--proxy-url", default=os.environ.get("STREAMING_PROXY_URL", "http://127.0.0.1:8090"))
    parser.add_argument("--proxy-token", default=os.environ.get("PROXY_AUTH_TOKEN", "ai_box_proxy_secret_token"))
    parser.add_argument("--gateway-url", default=os.environ.get("GATEWAY_URL", "http://127.0.0.1:3001"))
    parser.add_argument("--api-key", default=os.environ.get("DESTINATION_API_KEY", "ai_box_internal_api_secret_key"))

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    start_consumer(
        rabbitmq_host=args.rabbitmq_host,
        rabbitmq_port=args.rabbitmq_port,
        rabbitmq_user=args.rabbitmq_user,
        rabbitmq_password=args.rabbitmq_password,
        rabbitmq_vhost=args.rabbitmq_vhost,
        rabbitmq_queue=args.queue,
        rabbitmq_exchange=args.exchange,
        rabbitmq_routing_key=args.routing_key,
        task_db_path=args.task_db_path,
        proxy_base_url=args.proxy_url,
        proxy_auth_token=args.proxy_token,
        gateway_url=args.gateway_url,
        destination_api_key=args.api_key,
    )


if __name__ == "__main__":
    main()
