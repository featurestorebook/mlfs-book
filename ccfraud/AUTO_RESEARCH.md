# creditcard

This is an experiment to have the LLM do its own research.

## Setup

To set up a new experiment, work with the user to:

1. **Agree on a run tag**: propose a tag based on today's date (e.g. `mar5`). The branch `mlfs-book/<tag>` must not already exist — this is a fresh run.
2. **Create the branch**: `git checkout -b mlfs-book/<tag>` from current master.
3. **Read the in-scope files**: The repo is small. Read these files for full context:
   - `ccfraud/synth_transactions.py`: synthetic raw data for features
   - `notebooks/4-training-cc-fraud-pipeline.ipynb` — the file you modify. Model architecture, optimizer, training loop.
4. **Initialize results.tsv**: Create `results.tsv` with just the header row. The baseline will be recorded after the first run.
5. **Confirm and go**: Confirm setup looks good.

Once you get confirmation, kick off the experimentation.

## Experimentation

Each experiment runs locally. The training script runs for a **fixed time budget of 5 minutes** (wall clock training time, excluding startup/compilation). You launch it simply as: `uv run train_isolation_forest.py`.

**What you CAN do:**
- Modify `4-training-cc-fraud-pipeline.ipynb` — this is the only file you edit. Everything is fair game: model architecture, optimizer, hyperparameters, training loop, batch size, model size, etc.

**What you CANNOT do:**
- Install new packages or add dependencies. You can only use what's already in requirements.txt

**The goal is simple: get the lowest f1.** Since the time budget is fixed, you don't need to worry about training time — it's always 5 minutes. Everything is fair game: change the architecture, the optimizer, the hyperparameters, the batch size, the model size. The only constraint is that the code runs without crashing and finishes within the time budget.

**VRAM** is a soft constraint. Some increase is acceptable for meaningful gains, but it should not blow up dramatically.

**Simplicity criterion**: All else being equal, simpler is better. A small improvement that adds ugly complexity is not worth it. Conversely, removing something and getting equal or better results is a great outcome — that's a simplification win. When evaluating whether to keep a change, weigh the complexity cost against the improvement magnitude. An improvement of ~0 but much simpler code? Keep.

**The first run**: Your very first run should always be to establish the baseline, so you will run the training script as is.

## Output format

Once the script finishes it prints a summary like this:

```
---
n_test_samples
200000
roc_auc
0.5270871742875267
precision
0.3210332103321033
recall
0.05087719298245614
f1
0.08783442705704189
average_precision
0.0479756646453051
```

Note that the script is configured to always stop after 5 minutes, so depending on the computing platform of this computer the numbers might look different. 


## Logging results

When an experiment is done, register it with hopsworks model registry along with its metrics.


```
1. git commit hash (short, 7 chars)
2. f1 achieved (e.g. 0.07234567) — use 0.000000 for crashes
4. status: `keep`, `discard`, or `crash`
5. short text description of what this experiment tried

Example:

```
commit	f1	status	description
a1b2c3d	0.097900 keep	baseline
b2c3d4e	0.132000 keep	increase LR to 0.04
c3d4e5f	0.005000 discard change LR to 0.10
```

## The experiment loop

The experiment runs on a dedicated branch (e.g. `mlfs-book/mar5` or `mlfs-book/mar5-gpu0`).

LOOP FOREVER:

1. Look at the git state: the current branch/commit we're on
2. Tune `4-training-cc-fraud-pipeline.ipynb` with an experimental idea by directly hacking the code.
3. git commit
4. Run the experiment: `papermill notebooks/4-training-cc-fraud-pipeline.ipynb output_notebook.ipynb 2>&1`
5. Read out the results from the model registry for the model trained
6. If a run crashes and if you can't get things to work after more than a few attempts, give up.
7. Record the results in the model registry
8. If f1 improved (lower), you "advance" the branch, keeping the git commit
9. If f1 is equal or worse, you git reset back to where you started

The idea is that you are a completely autonomous researcher trying things out. If they work, keep. If they don't, discard. And you're advancing the branch so that you can iterate. If you feel like you're getting stuck in some way, you can rewind but you should probably do this very very sparingly (if ever).

**Timeout**: Each experiment should take ~5 minutes total (+ a few seconds for startup and eval overhead). If a run exceeds 10 minutes, kill it and treat it as a failure (discard and revert).

**Crashes**: If a run crashes (OOM, or a bug, or etc.), use your judgment: If it's something dumb and easy to fix (e.g. a typo, a missing import), fix it and re-run. If the idea itself is fundamentally broken, just skip it, log "crash" as the status in the tsv, and move on.

**NEVER STOP**: Once the experiment loop has begun (after the initial setup), do NOT pause to ask the human if you should continue. Do NOT ask "should I keep going?" or "is this a good stopping point?". The human might be asleep, or gone from a computer and expects you to continue working *indefinitely* until you are manually stopped. You are autonomous. If you run out of ideas, think harder — read papers referenced in the code, re-read the in-scope files for new angles, try combining previous near-misses, try more radical architectural changes. The loop runs until the human interrupts you, period.

As an example use case, a user might leave you running while they sleep. If each experiment takes you ~5 minutes then you can run approx 12/hour, for a total of about 100 over the duration of the average human sleep. The user then wakes up to experimental results, all completed by you while they slept!

