# llm-minder

**Run thousands of LLM calls concurrently without babysitting them.** When one
call hits a rate limit, every worker sharing that quota holds off on new calls
until the cooldown ends. Failures retry with the
right strategy for their cause. After a crash, a rerun resumes from its
checkpoint instead of starting over. Deadlines and spend caps stop a run cleanly,
and token counts include the attempts that failed.

It works with any async client: wrap your own, or use the built-in OpenAI,
Gemini, OpenRouter, DeepSeek, or PydanticAI support. Use it when you need results
during the current workflow; for latency-tolerant jobs, a provider's native batch
API may be cheaper.

[![PyPI version](https://img.shields.io/pypi/v/llm-minder)](https://pypi.org/project/llm-minder/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/llm-minder)](https://pepy.tech/projects/llm-minder)
[![Downloads as async-batch-llm](https://img.shields.io/pepy/dt/async-batch-llm?label=downloads%20as%20async-batch-llm)](https://pepy.tech/projects/async-batch-llm)
[![Python 3.11-3.15](https://img.shields.io/badge/python-3.11--3.15-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/github/actions/workflow/status/geoff-davis/llm-minder/test.yml?branch=main&label=tests)](https://github.com/geoff-davis/llm-minder/actions/workflows/test.yml)
[![Coverage](https://raw.githubusercontent.com/geoff-davis/llm-minder/python-coverage-comment-action-data/badge.svg)](https://github.com/geoff-davis/llm-minder/tree/python-coverage-comment-action-data)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/geoff-davis/llm-minder/blob/main/notebooks/llm_minder_quickstart.ipynb)

**[Documentation](https://geoff-davis.github.io/llm-minder/)** ·
[Getting started](https://geoff-davis.github.io/llm-minder/getting-started/) ·
[Examples](https://github.com/geoff-davis/llm-minder/tree/main/examples) ·
[Changelog](https://github.com/geoff-davis/llm-minder/blob/main/CHANGELOG.md)

> **Renamed:** this project was published as `async-batch-llm` through v0.32. From 1.0
> it is `llm-minder`, imported as `llm_minder`. Upgrading? Read the
> [1.0 migration guide](https://geoff-davis.github.io/llm-minder/migration/v1.0/)
> and see [Upgrading](#upgrading) below.

## Quick start

Install the OpenAI and terminal-progress extras, then set `OPENAI_API_KEY`:

```bash
pip install 'llm-minder[openai,progress]'
export OPENAI_API_KEY='...'
```

```python
import asyncio
from llm_minder import llm, process_prompts

async def main():
    batch = await process_prompts(
        llm("openai:gpt-6-luna"), ["Summarize A", "Summarize B"],
        concurrency=10, progress=True,
    )
    print(batch.summary())

asyncio.run(main())
```

`summary()` reports what happened, including retries and tokens (timings vary):

```text
Batch summary
=============
Items:     2 total — 2 succeeded, 0 failed
Stopped:   completed
Retries:   0 extra attempt(s) across 0 item(s)
Tokens:    in 240 (cached 0) · out 80
Wall time: 1.42s
  admission wait  p50 0.00s  p95 0.00s  p99 0.00s
  execution       p50 1.21s  p95 1.38s  p99 1.40s
```

No API key? [Run the credential-free demo](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_callable_application.py)
or [open the no-key notebook in Colab](https://colab.research.google.com/github/geoff-davis/llm-minder/blob/main/notebooks/llm_minder_quickstart.ipynb).

![Credential-free terminal demo](https://raw.githubusercontent.com/geoff-davis/llm-minder/main/docs/assets/v0.20-quickstart.gif)

## Typical jobs

**Labeling a large dataset.** You need 200,000 support tickets tagged by topic and
urgency. A quarter of the way in, the provider starts returning 429s, every
in-flight call retries at once, and the limit gets worse. When the script
finally dies, a restart pays for the first 50,000 calls again. With
llm-minder, workers that share a quota pause together and ramp back up,
the RPM and TPM limits you set are enforced before a call goes out, and with a
SQLite checkpoint and `ResumePolicy.REUSE_SUCCESSES`, a rerun reuses every
compatible success and runs the rest.
See [large runs](https://geoff-davis.github.io/llm-minder/large-runs/) and
[`example_production_resume.py`](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_production_resume.py).

**Extracting structured data.** You're pulling fields out of contracts or
invoices into a schema, and the cheap model occasionally returns JSON that
doesn't validate. Bad output and rate limits want different fixes: the first
might need a stronger model, the second only needs to wait. Rate limits draw on
their own retry budget, separate from other failures, and a strategy's
`on_error()` can escalate only the items that failed validation, so the
expensive model sees the hard cases and nothing else.
See [`example_smart_model_escalation.py`](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_smart_model_escalation.py).

**Comparing models on your own data.** Before switching models, you run the same
1,000 prompts through three providers and grade the answers. You want a result
per item, not a list with exceptions mixed in, and token counts that include
retries, so the cost comparison is honest. The
[GSM8K benchmark](https://geoff-davis.github.io/llm-minder/benchmarks/) in
this repo is built exactly this way, with an LLM judge as the fallback grader;
the [walkthrough](https://geoff-davis.github.io/llm-minder/examples/benchmark-walkthrough/)
shows the code.

**A nightly job with a deadline and a budget.** An enrichment job must finish
before the morning report and must not spend more than a set amount. A batch
deadline and token or cost caps stop the run cleanly, keep every result that
already finished, and record why the run stopped. With a checkpoint and a
resume policy, the next run reuses every compatible success and runs the rest.
See [guardrails](https://geoff-davis.github.io/llm-minder/guardrails/).

**Adding concurrency to an app you already have.** Your service already calls an
LLM through its own client or gateway, and you don't want a new SDK.
`CallableStrategy` wraps your existing async function, and `LLMCallPool` gives
concurrent requests in a long-running service one shared rate-limit cooldown
and a concurrency limit.
See [use your existing async client](#use-your-existing-async-client).

## Why not just `asyncio.gather`?

A semaphore plus `asyncio.gather()` caps concurrency, and for a small script
that is enough. Production batches need more:

| Problem | What llm-minder does |
| --- | --- |
| A 429 hits one call while 50 more are in flight | Coordinated cooldown: workers sharing the quota scope start no new calls or retries until it ends, then ramp back up; calls already in flight finish |
| Rate limits and bad outputs need different handling | Separate retry budgets for rate limits and for content/transport failures; `on_error()` lets a strategy escalate models |
| Request **and** token quotas (RPM + TPM) | Atomic per-scope admission with usage reconciliation and refunds |
| The input is a million rows, or the consumer is slow | Bounded input and result queues apply backpressure in both directions |
| The process dies at item 80,000 | JSONL or indexed SQLite checkpoints; resume replays only compatible results |
| A run must not exceed a time or money budget | Item deadlines, batch deadlines, token/cost caps, and category-based fail-fast |
| "How many tokens did that cost?" | Tokens and timing include retries and failed attempts |
| Exceptions mixed into a result list | Typed per-item results, lifecycle events, metrics, middleware, and progress callbacks |

For delayed, discounted processing, use a provider's native batch API. The
[scenario-based comparison](https://geoff-davis.github.io/llm-minder/comparison/)
covers Bespoke Curator, gateways, native batch APIs, and workflow engines.

## Benchmarks

Dated measurements from the GSM8K math benchmark, not provider guarantees:

- **Wall time** (June 2026). 30 serial calls took 39–65 seconds; with a worker pool they
  took 2.1–4.2 seconds on the two unthrottled providers.
- **Throughput** (June 2026). At the same concurrency over 1,000 items, the worker pool
  processed 72–108 items/s, against 55–58 items/s for a hand-written
  semaphore pool.
- **Model bake-off** (all 1,319 items, August 2026): DeepSeek V4 Flash scored
  96.9% for **$0.11**, Gemini 3.5 Flash-Lite 96.6% for **$0.71**, and GLM 5.3 Flash
  via OpenRouter 96.3% for **$0.03**.

The [benchmarks page](https://geoff-davis.github.io/llm-minder/benchmarks/)
has the methodology, model IDs, pricing snapshot, the throttled-provider case
where a bare `gather` was faster, and complete tables.

## Install

The core package depends only on `pydantic>=2.0` and `typing-extensions`. Add
extras for the providers you use:

| Extra | Enables | Installs |
| --- | --- | --- |
| `openai` | `OpenAIModel`, `OpenAICompatibleModel`, `llm("openai:…")`, `llm("openai-compatible:…")` | `openai>=1.66.2` |
| `openrouter` | `OpenRouterModel`, `llm("openrouter:…")` | `openai>=1.66.2` |
| `deepseek` | `DeepSeekModel`, `llm("deepseek:…")` | `openai>=1.66.2` |
| `gemini` | `GeminiModel`, `GeminiCachedModel`, `llm("gemini:…")` | `google-genai>=1.49.0` |
| `pydantic-ai` | `PydanticAIStrategy` | `pydantic-ai>=1.32.0` |
| `progress` | tqdm bars for `progress=True` | `tqdm>=4.66` |
| `media` | Image/video uploads to S3 or S3-compatible R2 storage | `boto3` |
| `all` | All of the above | All of the above |

`CallableStrategy`, `FakeStrategy`, and custom `LLMCallStrategy` subclasses need
no extra. Supported on Python 3.11–3.15, tested on Linux and macOS.

## Image and video uploads

Install `pip install 'llm-minder[media]'` to upload local images or videos to an
existing S3 or Cloudflare R2 bucket. Import the optional helper explicitly:

```python
from llm_minder.media import S3MediaUploader

async def upload_and_use(path, consume_url):
    # Credentials come from boto3's standard AWS credential chain.
    async with S3MediaUploader.from_boto3("your-bucket") as uploader:
        media = await uploader.upload(path)
        try:
            await consume_url(media.url)  # Your real application/provider client.
        finally:
            await uploader.delete(media)
```

`UploadedMedia` contains `bucket`, `key`, `url`, `content_type`, and `size` in
bytes. MIME types are inferred from the filename or supplied with
`upload(path, content_type="video/mp4")`; only `image/*` and `video/*` are accepted.
This is MIME labeling, not content inspection or format conversion. Each upload
gets a unique object key. Files stream from disk through bounded worker threads
(`max_concurrency=4` by default), using a single PUT for files up to 5 GiB.
Cancellation waits for its storage worker and attempts cleanup before releasing
its concurrency slot, so cancellation may take as long as the storage request.
Failed uploads attempt owned-object cleanup; cleanup failures remain visible on
the original exception. Successful uploads remain until explicitly deleted;
closing the uploader does not delete them. Keep the uploader alive until cleanup.

By default the URL is a private, presigned GET URL valid for `expires_in=3600`
seconds. Treat it as a bearer credential and allow enough lifetime for the
consumer to fetch it. Set `public_base_url="https://media.example.com/assets"`
only when that bucket/CDN path already exposes your objects publicly: the helper
does not change bucket access policies. Public object keys are URL-escaped.
Use least-privilege bucket permissions for PUT, GET/HEAD and DELETE; conditional
PUT/DELETE support is required to avoid overwriting or removing another object.

For R2, pass `endpoint_url="https://ACCOUNT_ID.r2.cloudflarestorage.com"` and
`region_name="auto"` to `from_boto3`. Supply credentials through environment
variables or the SDK credential chain, never committed source. An injected
`S3MediaUploader(client, "your-bucket")` leaves that client open; `from_boto3`
owns and closes its client. Use the uploader in one asyncio event loop.

Try the [runnable upload-and-cleanup example](https://github.com/liconstudiolab/llm-minder/blob/main/examples/example_media_upload.py):

```bash
uv sync --extra media
uv run python examples/example_media_upload.py photo.png clip.mp4 --bucket your-bucket --hold-seconds 60
```

The example makes real storage requests and deletes its objects afterward.
Uploading a video does **not** imply a model can consume it: choose a compatible
provider/model, use its media message format, and keep the URL accessible until
the request finishes. This helper does not change model or screening workflows.

## Providers

`llm("provider:model")` covers `openai:`, `gemini:`, `openrouter:`, `deepseek:`,
and `openai-compatible:` (any other OpenAI-compatible server, with `base_url=`).
Keyword arguments forward to the model constructor, for example
`llm("deepseek:deepseek-v4-flash", thinking=False, max_connections=150)`.

- **OpenAI** uses the Responses API by default (`api_surface="chat_completions"`
  opts out). OpenRouter, DeepSeek, and `OpenAICompatibleModel` share the same
  model layer; subclass it for Together, Fireworks, vLLM, and similar servers.
- **Gemini** supports structured response parsing and shared context caching.
- **PydanticAI** agents run through `PydanticAIStrategy` with typed output.
- **Anthropic** and anything else work through PydanticAI, `CallableStrategy`, or
  an `LLMCallStrategy` subclass.

For custom clients or cached models, use the explicit two-object form,
`OpenAIStrategy(OpenAIModel.from_api_key("gpt-6-luna"))`. See the
[provider guides](https://geoff-davis.github.io/llm-minder/),
[custom strategy guide](https://geoff-davis.github.io/llm-minder/examples/custom-strategies/),
and [OpenAI-compatible high-throughput guide](https://geoff-davis.github.io/llm-minder/openai-high-throughput/).
Model IDs and service limits change independently of this package, so check the
provider's documentation when choosing them.

## Use your existing async client

If your application already has an async client or gateway, wrap one call:

```python
from llm_minder import (
    ArtifactIdentity,
    CallOutcome,
    CallableStrategy,
    ProcessorConfig,
    process_stream,
)


async def invoke(prompt, *, attempt, timeout, state):
    response = await existing_client.generate(prompt, timeout=timeout)
    return CallOutcome(
        response.text,
        token_usage={
            "input_tokens": response.usage.prompt_tokens,
            "output_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        },
        metadata={"route": response.route},
    )


strategy = CallableStrategy(
    invoke,
    identity=ArtifactIdentity(provider="my-gateway", model="summary-route"),
)
config = ProcessorConfig(
    concurrency=32,
    max_queue_size=128,
    max_result_queue_size=64,
)

async for result in process_stream(strategy, database_prompt_source(), config=config):
    await save_result(result)
```

`CallableStrategy` runs on the same execution path as the built-in strategies,
so the call gets retries, cooldowns, deadlines, checkpoints, accounting, and
observers. `max_queue_size` bounds accepted input waiting for workers and
`max_result_queue_size` bounds finished results waiting for your loop; both
default to unbounded. See
[Use Your Existing Async Client](https://geoff-davis.github.io/llm-minder/callable-integration/).

## Choose an execution surface

| Need | API |
| --- | --- |
| Collect a finite run | `process_prompts()` |
| Handle results as they finish | `process_stream()` |
| Make one resilient request | `call()` / `call_result()` |
| Share limits across service requests | `LLMCallPool` |
| Customize queueing and lifecycle | `ParallelBatchProcessor` |

All five share the same retry, timing, admission, and token-accounting
pipeline. Pass `(item_id, prompt)` pairs to control IDs, or
`(item_id, prompt, context)` triples to carry application data into each result.
Results arrive in completion order; pass `preserve_order=True` to
`process_prompts()`, or call `batch.in_input_order()`, for submission order.
Exceptions the library defines subclass `LLMMinderError` and keep their
built-in bases (for example, `ItemDeadlineExceeded` is still a `TimeoutError`);
provider SDK errors are not wrapped. See the
[single-call and shared-call guide](https://geoff-davis.github.io/llm-minder/api/single-call-pool/)
and [core API](https://geoff-davis.github.io/llm-minder/api/core/).

## Production runs: checkpoints, deadlines, and budgets

The complete
[production resume example](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_production_resume.py)
is runnable. With your `strategy` and `prompts` defined, the core configuration
looks like this:

```python
from pathlib import Path

from llm_minder import (
    AbortMode,
    ArtifactIdentity,
    ErrorCategory,
    GuardrailConfig,
    JsonlArtifactStore,
    ProcessorConfig,
    ResumePolicy,
    process_prompts,
)

identity = ArtifactIdentity(
    provider="openai",
    model="gpt-6-luna",
    prompt_version="invoice-v4",
    parser_version="invoice-schema-v2",
    application_version="billing-pipeline-v7",
)
store = JsonlArtifactStore("runs/invoice-extraction.jsonl", identity=identity, fsync=True)
config = ProcessorConfig(
    concurrency=20,
    attempt_timeout=30,  # one provider attempt
    guardrails=GuardrailConfig(
        total_timeout_per_item=180,  # admission, waits, calls, and retries
        batch_timeout=3600,
        abort_on_error_categories=frozenset(
            {ErrorCategory.AUTHENTICATION, ErrorCategory.INSUFFICIENT_BALANCE}
        ),
        max_total_tokens=20_000_000,  # stop the run at a spend cap
        abort_mode=AbortMode.DRAIN_ACTIVE,
    ),
)

batch = await process_prompts(
    strategy,
    prompts,
    config=config,
    artifact_store=store,
    resume=ResumePolicy.REUSE_SUCCESSES,
    preserve_order=True,
)

if batch.termination.kind != "completed":
    print("controlled stop:", batch.termination)
Path("summary.json").write_text(batch.to_json(), encoding="utf-8")
```

What to know before relying on it:

- **Controlled stops return results.** Batch deadlines, budget stops, and
  fail-fast stops end the run with a `BatchResult` instead of an exception; check
  `batch.termination`. A fail-fast category triggers only once an item fails for
  good, not on a retryable attempt. Checkpoint write failures and cancellation by
  your own code still raise.
- **Budgets are soft caps.** Calls already running when the cap is reached
  finish (or are cancelled with `AbortMode.CANCEL_ACTIVE`), so usage can
  overshoot by at most one call per worker.
- **Checkpoints come before publication.** A result from a normal execution is
  appended and flushed before it is returned or streamed; `fsync=True` adds
  durability. Audit records for batch deadline, abort, and budget stops are
  best-effort, and items rejected before execution may have no record; see the
  [checkpoint failure rules](https://geoff-davis.github.io/llm-minder/results-and-artifacts/#item-local-artifact-serialization-failures).
  `JsonlArtifactStore` is safe for concurrent writes within one process, not
  across processes.
- **Replay is strict.** A stored result is reused only when the item ID, prompt,
  participating context, and the whole artifact identity match.
  `REUSE_SUCCESSES` reruns failures; `REUSE_ALL` also replays terminal provider
  failures. Deadline, abort, and budget stops, and configuration failures,
  always re-execute.
- **Privacy.** Raw prompts and contexts are not stored by default. Outputs and
  metadata are, and may contain sensitive application data.
- **Large runs.** `process_prompts()` keeps every result in memory. For bounded
  memory, use `process_stream()` with a lazy source and bounded queues. For 100k+
  items, also use the indexed SQLite backend: same records, same replay rules, no
  history decode on reopen. Pass the same `identity`:

  ```python
  from llm_minder import SqliteArtifactStore

  store = SqliteArtifactStore("runs/invoice-extraction.sqlite", identity=identity)
  ```

`attempt_timeout` limits one provider attempt;
`GuardrailConfig.total_timeout_per_item` limits the whole item, including
cooldowns, admission, retries, and backoff. The
[Choosing Your Limits guide](https://geoff-davis.github.io/llm-minder/choosing-your-limits/)
walks every limit in decision order with a worked 10k-item sizing example. See
also [Results, Artifacts, and Resume](https://geoff-davis.github.io/llm-minder/results-and-artifacts/),
[Large Runs](https://geoff-davis.github.io/llm-minder/large-runs/),
[Deadlines, Budgets and Fail-Fast Guardrails](https://geoff-davis.github.io/llm-minder/guardrails/),
and the [production checklist](https://geoff-davis.github.io/llm-minder/production-checklist/).

## Token-aware admission

For providers with both request and token quotas, enable token-aware admission
explicitly:

```python
from llm_minder import CharacterTokenEstimator, ProcessorConfig

config = ProcessorConfig(
    concurrency=32,
    max_requests_per_minute=500,
    max_tokens_per_minute=200_000,
    token_estimator=CharacterTokenEstimator(expected_output_tokens=400),
)
```

The [Token-Aware Admission guide](https://geoff-davis.github.io/llm-minder/token-aware-admission/)
explains estimators, shared quota scopes, refunds, underestimation debt, retries,
and known-zero versus unknown usage.

## Tokens and cost

`BatchResult` totals input, cached, output, and total tokens across retries,
including usage recovered from failed attempts. Retries hidden inside an
upstream gateway are counted only if the gateway reports their usage. The
package has no built-in price table; you supply the rates:

```python
cost = batch.estimated_cost(
    input_per_mtok=current_input_rate,
    output_per_mtok=current_output_rate,
    cached_token_rate=current_cache_rate,
)
```

To stop a run at a dollar amount, pass your pricing as
`GuardrailConfig(max_total_cost=..., cost_function=...)`; see the
[guardrails guide](https://geoff-davis.github.io/llm-minder/guardrails/#token-and-cost-budgets).

`WorkItemResult` and `BatchResult` serialize to strict, versioned JSON and JSONL;
unsupported values raise rather than fall back to `repr()`. See the
[artifact and serialization API](https://geoff-davis.github.io/llm-minder/api/artifacts/).

## Testing without provider calls

Use `FakeStrategy` without provider extras or credentials:

```python
from llm_minder import process_prompts
from llm_minder.testing import FakeStrategy

batch = await process_prompts(
    FakeStrategy(lambda prompt: prompt.upper(), token_usage={"input_tokens": 2, "output_tokens": 1}),
    ["hello", "world"],
)
assert list(batch.in_input_order().outputs()) == ["HELLO", "WORLD"]
```

`FakeStrategy` and `MockAgent` also simulate latency, rate limits, retryable
failures, and terminal failures without spending API quota. The project's own
test suite makes no live provider calls. See the
[testing guide](https://geoff-davis.github.io/llm-minder/testing/).

## Examples

Start with these runnable examples:

- [Existing async application client, bounded streaming, and replay](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_callable_application.py)
- [Production checkpoints and guardrails](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_production_resume.py)
- [OpenAI batch processing](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_openai.py)
- [Single calls and a shared call pool](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_gateway.py)
- [Validation-aware model escalation](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_smart_model_escalation.py)
- [Custom embedding strategies](https://github.com/geoff-davis/llm-minder/blob/main/examples/example_embeddings.py)

The [examples directory](https://github.com/geoff-davis/llm-minder/tree/main/examples)
also covers Gemini, DeepSeek, OpenRouter, Anthropic, LangChain, caching,
grounding, and the benchmark.

## Documentation

- [Getting Started](https://geoff-davis.github.io/llm-minder/getting-started/)
- [Compare Alternatives](https://geoff-davis.github.io/llm-minder/comparison/)
- [Choosing Your Limits](https://geoff-davis.github.io/llm-minder/choosing-your-limits/)
- [Production Checklist](https://geoff-davis.github.io/llm-minder/production-checklist/)
- [Troubleshooting and FAQ](https://geoff-davis.github.io/llm-minder/troubleshooting/)
- [Logging](https://geoff-davis.github.io/llm-minder/logging/)
- [Results, Artifacts, and Resume](https://geoff-davis.github.io/llm-minder/results-and-artifacts/)
- [Deadlines, Budgets and Fail-Fast Guardrails](https://geoff-davis.github.io/llm-minder/guardrails/)
- [Bounded Work and Backpressure](https://geoff-davis.github.io/llm-minder/bounded-work/)
- [API Reference](https://geoff-davis.github.io/llm-minder/api/core/)
- [API Stability (draft for 1.0)](https://geoff-davis.github.io/llm-minder/stability/)

## Upgrading

From v0.32, read the
[1.0 migration guide](https://geoff-davis.github.io/llm-minder/migration/v1.0/). The
package is renamed to `llm-minder`, Python 3.11 is required, everything 0.27 and 0.28
deprecated is removed, truncated responses get their own error category, and a
cut-off or blocked empty reply now fails instead of succeeding. Coming from an older
release, upgrade to v0.32 first with the guides below.

From v0.27, read the
[v0.28 migration guide](https://geoff-davis.github.io/llm-minder/migration/v0.28/):
v0.28 only adds deprecation warnings, for positional arguments to the configuration
classes and the no-op `enable_detailed_logging`.

From v0.26, read the
[v0.27 migration guide](https://geoff-davis.github.io/llm-minder/migration/v0.27/)
first. In short: `OpenAIModel` now uses the Responses API, built-in models no longer
send a default temperature, application errors are no longer mistaken for rate
limits, and several names are deprecated ahead of 1.0. Run your tests with
`-W error::DeprecationWarning` to find them (on Python 3.14 with google-genai
installed, also pass `-W "ignore::DeprecationWarning:google.genai.types"`). The
[API stability page](https://geoff-davis.github.io/llm-minder/stability/)
lists what 1.0 keeps stable.

v0.32 is the last release that supports Python 3.10; 1.0 requires Python 3.11
or newer. Guides for earlier versions are in the
[migration section](https://geoff-davis.github.io/llm-minder/migration/v0.28/)
of the documentation.

## Contributing

Clone the repository and use its pinned development environment:

```bash
git clone https://github.com/geoff-davis/llm-minder.git
cd llm-minder
uv sync --all-extras
make ci
```

See the [contributing guide](https://geoff-davis.github.io/llm-minder/contributing/)
or open an [issue](https://github.com/geoff-davis/llm-minder/issues). To
report a security vulnerability, follow the
[security policy](https://github.com/geoff-davis/llm-minder/blob/main/SECURITY.md)
rather than opening a public issue.

## License

MIT License. See [LICENSE](https://github.com/geoff-davis/llm-minder/blob/main/LICENSE).
