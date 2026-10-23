"""Adapter: wraps the PDF bot's async ``embedqa.answer_events()`` or
``retrieve.answer_events()`` and translates its output into the SSE event
format the email chatbot frontend already understands.

Pipeline selection
------------------
Set ``DOC_PIPELINE`` in ``.env``:

* ``full``   (default) — ``embedqa.py``.  Extracts metadata filters (company,
  year, quarter), builds a scoped facts_view, hybrid search (vector + keyword),
  tool-calling loop with run_metric + calculate, number verification.
  More accurate, slower (~30-60 s on CPU).

* ``simple`` — ``retrieve.py``.  Embedding + keyword hybrid search only, no
  metadata filters, no facts_view, no run_metric.  Just top-K passages fed
  straight to the chat model with a calculator tool.  Faster (~10-20 s on CPU),
  good enough for narrative / "what does the report say" questions.

The PDF bot now runs on asyncpg + Weaviate (not SQLite); every function here is async.
"""
import json
import time
import threading
import traceback

from . import embedqa
from . import retrieve as retrieve_pipeline
from . import store
from . import config
from core.logging_config import get_agent_logger
from core.utils import sanitize_log_input

logger = get_agent_logger("chatbot")


def _use_simple() -> bool:
    """Return True when the operator chose the lightweight retrieve pipeline."""
    return config.DOC_PIPELINE == "simple"

async def stream_answer(query: str, history: list[dict] | None = None,
                        doc_ids: list[str] | None = None,
                        cancel_event: threading.Event | None = None):
    """Async generator yielding SSE-formatted strings.

    FastAPI's ``StreamingResponse`` handles async generators natively — callers simply pass
    the result of this function to ``StreamingResponse(..., media_type='text/event-stream')``.

    When *cancel_event* is set the pipeline stops between rounds / tool calls and this
    generator terminates without emitting an answer event.
    """
    t0 = time.perf_counter()
    sources: list[dict] = []
    tool_trace: list[dict] = []
    event_count = 0

    logger.info("[DOC-SVC] stream_answer() START  query='%s'  history=%d  doc_ids=%s",
                sanitize_log_input(query[:120]), len(history or []), doc_ids)

    try:
        simple = _use_simple()
        pipeline_mod = retrieve_pipeline if simple else embedqa
        pipeline_label = "retrieve (simple)" if simple else "embedqa (full)"

        logger.info("[DOC-SVC] Calling %s.answer_events()  model=%s  reasoning=False",
                    pipeline_label, config.CHAT_MODEL)

        events_kwargs: dict = dict(
            question=query,
            history=history,
            doc_ids=doc_ids,
            model=config.CHAT_MODEL,
            reasoning=False,
            cancel_event=cancel_event,
        )
        if not simple:
            events_kwargs["style"] = "full"

        async for kind, val in pipeline_mod.answer_events(**events_kwargs):
            event_count += 1
            elapsed = (time.perf_counter() - t0) * 1000

            # ── Stop early if cancelled ──
            if cancel_event is not None and cancel_event.is_set():
                logger.info("[DOC-SVC] cancel_event set — stopping stream_answer")
                return
            elif kind == "series":
                logger.info("[DOC-SVC] event #%d [series] %.0fms: %d series", event_count, elapsed,
                            len(val) if val else 0)

            elif kind == "answer":
                logger.info("[DOC-SVC] event #%d [answer] %.0fms: %d chars",
                            event_count, elapsed, len(val) if val else 0)
                yield f"data: {json.dumps({'type': 'token', 'content': val})}\n\n"

    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        logger.error("[DOC-SVC] stream_answer() EXCEPTION after %.0fms and %d events: %s\n%s",
                     elapsed, event_count, e, traceback.format_exc())
        # Return a generic message to the client; the exception detail /
        # stack trace is logged server-side only (CWE-209).
        yield f"data: {json.dumps({'type': 'error', 'content': 'Document search failed. Please try again later.'})}\n\n"
        return

    elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.info("[DOC-SVC] stream_answer() DONE  %.0fms  events=%d  sources=%d  tools=%d",
                elapsed_ms, event_count, len(sources), len(tool_trace))
    yield f"data: {json.dumps({'type': 'done', 'timings': {'total': round(elapsed_ms, 1)}, 'follow_up_questions': [], 'source_mode': 'documents'})}\n\n"


async def answer_once(query: str, history: list[dict] | None = None,
                      doc_ids: list[str] | None = None) -> dict:
    """Non-streaming counterpart to ``stream_answer`` for ``POST /chatbot/query``."""
    t0 = time.perf_counter()
    sources: list[dict] = []
    answer_text = ""
    event_count = 0

    logger.info("[DOC-SVC] answer_once() START  query='%s'  history=%d  doc_ids=%s",
                sanitize_log_input(query[:120]), len(history or []), doc_ids)

    try:
        simple = _use_simple()
        pipeline_mod = retrieve_pipeline if simple else embedqa

        events_kwargs: dict = dict(
            question=query,
            history=history,
            doc_ids=doc_ids,
            model=config.CHAT_MODEL,
            reasoning=False,
        )
        if not simple:
            events_kwargs["style"] = "full"

        async for kind, val in pipeline_mod.answer_events(**events_kwargs):
            event_count += 1
            if kind == "passages":
                sources = [_passage_to_source(p, i) for i, p in enumerate(val)]
            elif kind == "answer":
                answer_text = val or ""
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        logger.error("[DOC-SVC] answer_once() EXCEPTION after %.0fms and %d events: %s\n%s",
                     elapsed, event_count, e, traceback.format_exc())
        # Return a generic message to the client; the exception detail /
        # stack trace is logged server-side only (CWE-209).
        return {
            "answer": "Document search failed. Please try again later.",
            "sources": [],
            "timings": {"total": round(elapsed, 1)},
        }

    elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.info("[DOC-SVC] answer_once() DONE  %.0fms  events=%d  sources=%d  answer_len=%d",
                elapsed_ms, event_count, len(sources), len(answer_text))
    return {
        "answer": answer_text,
        "sources": sources,
        "timings": {"total": round(elapsed_ms, 1)},
    }


async def get_document_names() -> list[str]:
    """Return file names of all ready documents in the PDF library."""
    try:
        async with store.acquire() as conn:
            docs = await store.documents(conn)
        names = [d["file_name"] for d in docs if d["status"] == "ready"]
        logger.info("[DOC-SVC] get_document_names(): %d ready documents", len(names))
        return names
    except Exception as e:
        logger.error("[DOC-SVC] get_document_names() failed: %s", e)
        return []


async def is_available() -> bool:
    """Check if the document module is functional (PostgreSQL pool reachable)."""
    try:
        async with store.acquire() as conn:
            docs = await store.documents(conn)
        logger.info("[DOC-SVC] is_available(): True (%d documents in library)", len(docs))
        return True
    except Exception as e:
        logger.warning("[DOC-SVC] is_available(): False — %s", e)
        return False
