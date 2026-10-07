#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Live Transaction Generator (Hopsworks job)

A long-running Python job that writes a configurable number of synthetic credit
card transactions per minute (default: 100) to the `credit_card_transactions`
feature group, and the labels of the fraudulent ones to `cc_fraud`.

Every write to `credit_card_transactions` (an online-enabled feature group) goes
through its Kafka topic, which is the source for the Spark Structured Streaming
feature pipeline (2-spark-streaming-feature-pipeline.py).

Fraud is injected as two patterns, matching the backfill data:
  - chain attacks: a burst of 5-12 small-then-larger online transactions from one
    foreign IP address, spread over a few minutes
  - geographic fraud: a card-present transaction in a far-away country only
    minutes after a card-present transaction in the cardholder's home country

Run `1_data_generator.py --mode backfill` first: this job samples from the cards,
accounts and merchants it created.

Run as a Hopsworks job (stop it with `hops job stop ccfraud-transactions`):
    hops job deploy ccfraud-transactions <path>/ccfraud/1b-transaction-generator-job.py \
        --env python-feature-pipeline --args "--transactions-per-min 100" --run

Or locally / in a terminal:
    python ccfraud/1b-transaction-generator-job.py --transactions-per-min 100
"""

import argparse
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import hopsworks
import numpy as np
import polars as pl

# __file__ is ccfraud/ccfraud/1b-transaction-generator-job.py; in a Hopsworks job it runs
# in place from the HopsFS mount, so the repo's packages are importable the same way.
current_file = Path(__file__).absolute()
ccfraud_project_dir = current_file.parent.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

from ccfraud import synth_transactions as st  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Write a live stream of synthetic credit card transactions to Hopsworks"
    )
    parser.add_argument("--transactions-per-min", type=int, default=100,
                        help="Legitimate transactions written per minute (default: 100)")
    parser.add_argument("--fraud-rate", type=float, default=0.005,
                        help="Fraudulent transactions as a fraction of all transactions (default: 0.005)")
    parser.add_argument("--chain-attack-ratio", type=float, default=0.9,
                        help="Fraction of fraud that is chain attacks vs geographic fraud (default: 0.9)")
    parser.add_argument("--tick-seconds", type=float, default=60.0,
                        help="Seconds between writes; each write holds rate x tick transactions. Every write is "
                             "also a commit to the offline Delta table, so keep it >= 10 (default: 60)")
    parser.add_argument("--duration-mins", type=float, default=0,
                        help="Stop after this many minutes; 0 runs until the job is stopped (default: 0)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed (default: random, so restarts do not replay the same stream)")
    parser.add_argument("--env-file", type=str, default=None,
                        help="Path to .env file, when running outside Hopsworks (default: <root>/.env)")
    args = parser.parse_args(argv)
    if args.transactions_per_min < 1:
        parser.error("--transactions-per-min must be >= 1")
    if not 0 <= args.fraud_rate < 1:
        parser.error("--fraud-rate must be in [0, 1)")
    return args


def utcnow() -> datetime:
    """Naive UTC timestamp; hsfs serializes naive datetimes as UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class LiveTransactionGenerator:
    """Writes `transactions_per_min` transactions per minute, in ticks of `tick_seconds`."""

    def __init__(self, args):
        self.args = args
        self.rng = np.random.default_rng(args.seed)
        self.shutdown_requested = False
        self.last_ip_per_card = {}  # cc_num -> (ip_address, country)
        self.pending_fraud = []  # scheduled fraudulent transactions, emitted when due
        self.legit_carry = 0.0  # fractional transactions carried to the next tick
        self.fraud_budget = 0.0  # fraudulent transactions owed, by fraud rate
        self.last_t_id = 0
        self.total_transactions = 0
        self.total_fraud = 0

        env_file = Path(args.env_file) if args.env_file else root_dir / ".env"
        if env_file.exists():
            # Outside Hopsworks: the .env file provides HOPSWORKS_API_KEY etc.
            from mlfs import config
            config.HopsworksSettings(_env_file=str(env_file))

        self.project = hopsworks.login()
        self.fs = self.project.get_feature_store()
        print(f"Connected to project: {self.project.name}")

        self._load_entities()
        self.transactions_fg = self.fs.get_feature_group("credit_card_transactions", version=1)
        self.fraud_fg = self.fs.get_feature_group("cc_fraud", version=1)
        # This job writes every minute: with statistics on, every write would start a Spark
        # statistics job. Turn them off on tables created before statistics_config=False.
        for fg in (self.transactions_fg, self.fraud_fg):
            if fg.statistics_config.enabled:
                fg.statistics_config = False
                fg.update_statistics_config()

        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)

    def _request_shutdown(self, sig, frame):
        print(f"\nSignal {sig} received, finishing the current tick...")
        self.shutdown_requested = True

    def _load_entities(self):
        """Load the cards, accounts and merchants written by the backfill."""
        try:
            merchants = pl.from_pandas(self.fs.get_feature_group("merchant_details", version=1).read())
            accounts = pl.from_pandas(self.fs.get_feature_group("account_details", version=1).read())
            cards = pl.from_pandas(self.fs.get_feature_group("card_details", version=1).read())
        except Exception as e:
            print(f"ERROR: could not load entities ({e}). Run the backfill first:\n"
                  "  python ccfraud/1_data_generator.py --mode backfill")
            sys.exit(1)

        if "home_country" not in accounts.columns:
            accounts = st.assign_cardholder_home_locations(accounts, seed=42)
        # card_details can hold several versions of a card: keep the latest one
        cards = cards.sort("last_modified").unique(subset=["cc_num"], keep="last")
        cards = cards.join(accounts.select(["account_id", "home_country"]), on="account_id", how="left")
        cards = cards.with_columns(pl.col("home_country").fill_null("United States"))

        self.cc_nums = cards["cc_num"].to_numpy()
        self.account_ids = cards["account_id"].to_numpy()
        self.home_countries = cards["home_country"].to_numpy()
        self.merchant_ids = merchants["merchant_id"].to_numpy()
        self.countries = np.array(list(st.COUNTRY_IP_RANGES.keys()))
        print(f"Loaded {len(self.cc_nums)} cards, {accounts.height} accounts, {len(self.merchant_ids)} merchants")

    def _next_t_ids(self, n: int) -> np.ndarray:
        """Transaction ids are microseconds since the epoch, so they never collide with the
        backfill's ids (0..num_transactions) and a restarted job needs no lookup of the max id."""
        start = max(int(time.time() * 1_000_000), self.last_t_id + 1)
        self.last_t_id = start + n - 1
        return np.arange(start, start + n, dtype=np.int64)

    def _ip_for(self, cc_num: str, home_country: str):
        """Location continuity: 60% reuse the card's last IP, else 85% home country, 15% abroad."""
        if cc_num in self.last_ip_per_card and self.rng.random() < 0.6:
            return self.last_ip_per_card[cc_num]
        country = home_country if self.rng.random() < 0.85 else str(self.rng.choice(self.countries))
        ip = st.generate_ip_for_country(country, seed=int(self.rng.integers(2**31)))
        self.last_ip_per_card[cc_num] = (ip, country)
        return ip, country

    def _legit_transactions(self, n: int, tick_start: datetime, tick_end: datetime) -> pl.DataFrame:
        card_idx = self.rng.integers(0, len(self.cc_nums), size=n)
        tick_us = int((tick_end - tick_start).total_seconds() * 1_000_000)
        offsets = np.sort(self.rng.integers(0, max(tick_us, 1), size=n))
        ips = [self._ip_for(self.cc_nums[i], self.home_countries[i])[0] for i in card_idx]
        return pl.DataFrame({
            "t_id": self._next_t_ids(n),
            "cc_num": self.cc_nums[card_idx],
            "account_id": self.account_ids[card_idx],
            "merchant_id": self.rng.choice(self.merchant_ids, size=n),
            "amount": np.round(self.rng.lognormal(mean=3.5, sigma=1.2, size=n), 2),
            "ip_address": ips,
            "card_present": self.rng.random(n) < 0.3,
            "ts": [tick_start + timedelta(microseconds=int(o)) for o in offsets],
        })

    def _schedule_chain_attack(self, now: datetime) -> int:
        """A burst of 5-12 online transactions from one foreign IP over the next few minutes:
        small 'testing' amounts first, then larger ones just under a typical review limit."""
        i = int(self.rng.integers(len(self.cc_nums)))
        cc_num, home = self.cc_nums[i], self.home_countries[i]
        foreign = [c for c in self.countries if c != home]
        country = str(self.rng.choice(foreign))
        ip = st.generate_ip_for_country(country, seed=int(self.rng.integers(2**31)))
        n = int(self.rng.integers(5, 13))
        # 40% of attacks are tight bursts (seconds apart), the rest spread over minutes
        gaps = self.rng.uniform(2, 20, n) if self.rng.random() < 0.4 else self.rng.uniform(10, 90, n)
        at = now
        for k in range(n):
            at = at + timedelta(seconds=float(gaps[k]))
            if k < n // 3:
                amount = self.rng.uniform(1.0, 3.0)
            elif k < 2 * n // 3:
                amount = self.rng.uniform(5.0, 15.0)
            else:
                amount = self.rng.uniform(35.0, 49.99)
            self.pending_fraud.append({
                "due": at, "cc_num": cc_num, "account_id": self.account_ids[i],
                "merchant_id": str(self.rng.choice(self.merchant_ids)), "amount": round(float(amount), 2),
                "ip_address": ip, "card_present": False,
                "explanation": f"Chain attack: Multiple small transactions (${amount:.2f}) in short time period",
            })
        return n

    def _schedule_geographic_fraud(self, now: datetime) -> int:
        """A card-present transaction at home, then a card-present one far away minutes later.
        Only the second one is fraud."""
        i = int(self.rng.integers(len(self.cc_nums)))
        cc_num, home = self.cc_nums[i], self.home_countries[i]
        far = str(self.rng.choice([c for c in self.countries if c != home]))
        gap_mins = int(self.rng.integers(5, 60))
        common = {"cc_num": cc_num, "account_id": self.account_ids[i], "card_present": True}
        self.pending_fraud.append({
            **common, "due": now, "merchant_id": str(self.rng.choice(self.merchant_ids)),
            "amount": round(float(self.rng.lognormal(3.5, 1.0)), 2),
            "ip_address": st.generate_ip_for_country(home, seed=int(self.rng.integers(2**31))),
            "explanation": None,  # the legitimate transaction at home
        })
        self.pending_fraud.append({
            **common, "due": now + timedelta(minutes=gap_mins),
            "merchant_id": str(self.rng.choice(self.merchant_ids)),
            "amount": round(float(self.rng.uniform(200, 2000)), 2),
            "ip_address": st.generate_ip_for_country(far, seed=int(self.rng.integers(2**31))),
            "explanation": f"Geographic fraud: Card present transaction in {far}, preceded by card present "
                           f"transaction in {home} only {gap_mins} minutes earlier (impossible travel)",
        })
        return 1

    def _due_fraud(self, tick_end: datetime):
        """Pop the scheduled (fraud-pattern) transactions due by tick_end."""
        due = [r for r in self.pending_fraud if r["due"] < tick_end]
        self.pending_fraud = [r for r in self.pending_fraud if r["due"] >= tick_end]
        if not due:
            return None, None
        t_ids = self._next_t_ids(len(due))
        txns = pl.DataFrame({
            "t_id": t_ids,
            "cc_num": [r["cc_num"] for r in due],
            "account_id": [r["account_id"] for r in due],
            "merchant_id": [r["merchant_id"] for r in due],
            "amount": [r["amount"] for r in due],
            "ip_address": [r["ip_address"] for r in due],
            "card_present": [r["card_present"] for r in due],
            "ts": [r["due"] for r in due],
        })
        labels = pl.DataFrame({
            "t_id": t_ids,
            "cc_num": [r["cc_num"] for r in due],
            "explanation": [r["explanation"] for r in due],
            "ts": [r["due"] for r in due],
        }).filter(pl.col("explanation").is_not_null())
        return txns, labels

    def tick(self, tick_start: datetime, tick_end: datetime):
        tick_s = (tick_end - tick_start).total_seconds()
        self.legit_carry += self.args.transactions_per_min * tick_s / 60.0
        n_legit = int(self.legit_carry)
        self.legit_carry -= n_legit

        # Fraud is owed at fraud_rate of all transactions; start a new attack once enough is owed
        self.fraud_budget += n_legit * self.args.fraud_rate / (1 - self.args.fraud_rate)
        while self.fraud_budget >= 1:
            if self.rng.random() < self.args.chain_attack_ratio:
                self.fraud_budget -= self._schedule_chain_attack(tick_end)
            else:
                self.fraud_budget -= self._schedule_geographic_fraud(tick_end)

        frames = [self._legit_transactions(n_legit, tick_start, tick_end)] if n_legit else []
        fraud_txns, labels = self._due_fraud(tick_end)
        if fraud_txns is not None:
            frames.append(fraud_txns)
        if not frames:
            return
        txns = pl.concat(frames).sort("ts")

        self.transactions_fg.multi_part_insert(txns.to_pandas())
        if labels is not None and labels.height > 0:
            self.fraud_fg.insert(labels.to_pandas(), wait=False)
        self.total_transactions += txns.height
        self.total_fraud += 0 if labels is None else labels.height

    def run(self):
        tick = timedelta(seconds=self.args.tick_seconds)
        print(f"Writing {self.args.transactions_per_min} transactions/min in {self.args.tick_seconds}s ticks, "
              f"fraud rate {self.args.fraud_rate:.2%} -> credit_card_transactions (Kafka)")
        start = time.monotonic()
        deadline = start + self.args.duration_mins * 60 if self.args.duration_mins > 0 else None
        tick_start = utcnow()
        n_ticks = 0
        try:
            while not self.shutdown_requested and (deadline is None or time.monotonic() < deadline):
                # Sleep until the tick's end so every transaction timestamp is in the past
                time.sleep(max(0.0, start + (n_ticks + 1) * self.args.tick_seconds - time.monotonic()))
                tick_end = tick_start + tick
                try:
                    self.tick(tick_start, tick_end)
                except Exception as e:
                    print(f"Warning: tick failed, skipping it: {e}")
                tick_start = tick_end
                n_ticks += 1
                if n_ticks % max(1, int(60 / self.args.tick_seconds)) == 0:
                    mins = (time.monotonic() - start) / 60
                    print(f"[{mins:6.1f} min] {self.total_transactions:,} transactions "
                          f"({self.total_transactions / mins:.0f}/min), {self.total_fraud:,} fraud")
                if len(self.last_ip_per_card) > 50_000:
                    self.last_ip_per_card.clear()
        finally:
            self.transactions_fg.finalize_multi_part_insert()
            print(f"Stopped: {self.total_transactions:,} transactions, {self.total_fraud:,} fraud labels written")


def main(argv=None):
    LiveTransactionGenerator(parse_args(argv)).run()


if __name__ == "__main__":
    main()
