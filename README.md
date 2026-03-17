# tap-amplitude

Singer tap for [Amplitude](https://amplitude.com), built with the [Meltano Singer SDK](https://sdk.meltano.com).

Streams extracted:

| Stream | Replication | Key |
|--------|-------------|-----|
| `events` | Incremental (`event_time`) | `uuid` |

---

## Configuration

| Setting | Required | Description |
|---------|----------|-------------|
| `api_key` | ✅ | Amplitude API Key — found in *Project Settings → API Credentials* |
| `secret_key` | ✅ | Amplitude Secret Key — found in *Project Settings → API Credentials* |
| `start_date` | | Earliest date to sync, e.g. `2024-01-01T00:00:00Z`. Defaults to 30 days ago. |

---

## Usage

### With Meltano

#### 1. Add to `meltano.yml`

```yaml
plugins:
  extractors:
    - name: tap-amplitude
      namespace: tap_amplitude
      pip_url: git+https://github.com/graet/tap-amplitude.git
      executable: tap-amplitude
      capabilities:
        - state
        - catalog
        - discover
      settings:
        - name: api_key
          kind: password
        - name: secret_key
          kind: password
        - name: start_date
          kind: date_iso8601
      config:
        start_date: "2024-01-01T00:00:00Z"
```

#### 2. Set credentials

```bash
meltano config tap-amplitude set api_key     YOUR_API_KEY
meltano config tap-amplitude set secret_key  YOUR_SECRET_KEY
```

#### 3. Run

```bash
meltano run tap-amplitude target-your-loader
```

### Standalone

#### 1. Install

```bash
pip install .
```

#### 2. Create `config.json`

Create a `config.json` file in your working directory:

```json
{
  "api_key": "YOUR_AMPLITUDE_API_KEY",
  "secret_key": "YOUR_AMPLITUDE_SECRET_KEY",
  "start_date": "2024-01-01T00:00:00Z"
}
```

> `config.json` is listed in `.gitignore` and will not be committed accidentally.

#### 3. Run

```bash
# Discover the stream schema
tap-amplitude --discover --config config.json

# Run a full sync
tap-amplitude --config config.json

# Run an incremental sync (pass previous state)
tap-amplitude --config config.json --state state.json
```

---

## Development & testing

### Setup

```bash
# create a virtual environment
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# install the package and test dependencies
pip install -e ".[testing]"
```

### Run the tests

```bash
pytest tests/ -v
```

All tests use mocked HTTP responses — no Amplitude credentials or network access required.

### Test structure

| File | What it covers |
|------|----------------|
| `tests/test_client.py` | `AmplitudeClient.export()` — zip/gzip parsing, 404 handling, corrupt responses, HTTP errors |
| `tests/test_streams.py` | `EventsStream` helpers — timestamp normalisation, start-date resolution from bookmark/config/default, day-chunking logic |
| `tests/test_tap.py` | `TapAmplitude` — stream discovery, required/optional config validation |
