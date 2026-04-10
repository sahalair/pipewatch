# pipewatch

A lightweight CLI tool to monitor and diff data pipeline outputs across runs with structured logging support.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Run pipewatch against your pipeline output to capture a snapshot:

```bash
pipewatch run --pipeline my_pipeline --output results/output.json
```

Diff two pipeline runs to spot changes:

```bash
pipewatch diff --run-a runs/2024-01-10 --run-b runs/2024-01-11
```

Enable structured JSON logging for downstream processing:

```bash
pipewatch run --pipeline my_pipeline --log-format json --log-file pipeline.log
```

### Example Output

```
[pipewatch] Run comparison: 2024-01-10 vs 2024-01-11
  ✔ schema        no changes
  ✗ row_count     1024 → 1031 (+7)
  ✗ null_rate     0.02 → 0.05 (+0.03)
  ✔ column_names  no changes
```

---

## Features

- Snapshot and compare pipeline outputs across runs
- Structured logging with JSON support
- Lightweight with minimal dependencies
- Easy integration into CI/CD workflows

---

## License

This project is licensed under the [MIT License](LICENSE).