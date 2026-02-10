# PyJKPTG-API

A FastAPI application for querying JKPTG (Jabatan Ketua Pengarah Tanah dan Galian) shiplist and license data using DuckDB and Parquet files.

## Features

- Query ship and license records from Parquet files
- Filter by concession, company, region, status, and more
- Track expiring licenses with days remaining
- Coordinates nested under a single `coordinate` object for license data

## Tech Stack

- **FastAPI** - Web framework
- **DuckDB** - In-memory SQL analytics on Parquet files
- **Pydantic** - Response schema validation

## Quick Start

### Docker

```bash
docker build -t jkptg-api .
docker run -p 8000:8000 jkptg-api
```

### Local Development

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Access the API

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Shiplist

| Endpoint | Description |
|---|---|
| `GET /shiplist` | Query ships with filters: `concession`, `company`, `file_no`, `license_no` |
| `GET /shiplist/expiring` | All ships sorted by expiry date with `daysRemaining` |
| `GET /companies/{file_no}` | Get ships by file number |
| `GET /concessions` | List all distinct concessions |
| `GET /concessions/{name}` | Get ships by concession name |

### Licenselist

| Endpoint | Description |
|---|---|
| `GET /licenselist` | Query licenses with filters: `region`, `company`, `license_no`, `status`, `no_file` |
| `GET /licenselist/expiring` | All licenses sorted by expiry date with `daysRemaining` |
| `GET /regions` | List all distinct regions |
| `GET /regions/{name}` | Get licenses by region name |

## Project Structure

```
PyJKPTG-API/
├── main.py             # FastAPI app with all endpoints and schemas
├── data/
│   ├── Shiplist-20260112.parquet
│   └── Licenselist-20260122.parquet
├── requirements.txt    # Python dependencies
├── Dockerfile          # Docker image definition
└── README.md
```

## License

Private
